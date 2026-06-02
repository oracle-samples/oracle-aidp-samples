---
id: external-catalog-system-table-hardening
name: External-catalog system table — initialisation, durability, and notification hygiene
type: Bug Fix
priority: P1
effort: Medium
impact: High
created: 2026-05-08
---

# External-catalog system table — initialisation, durability, and notification hygiene

## Problem Statement

Four operationally distinct issues, all on the same code path
(`qualifire/storage/external_catalog.py` + the engine's persistence
+ notification flow), surfaced while configuring qualifire against
an AIDP-style external catalog (e.g. Oracle 23ai vector DB at
`vectordb23ai.admin.qualifire_history`). They share enough wiring
that fixing them in one feature is more efficient than four
sequential PRs.

### 1. Object-storage staging hop on every write to in-database catalogs

When the `system_table_backend` is an external catalog whose tables
live **inside a database** (Oracle 23ai vector DB, ADW, similar),
the AIDP / `datalake-connectivity` spark-connectors stage every
write through object storage by default before pushing into the
database. For the qualifire system table that's pure overhead —
small, frequent inserts that never need staging — and it can fail
outright when the Spark session has no object-store credentials.

`datalake-connectivity` exposes a per-write option that bypasses
the staging hop:

- `connectivity/spark-connectors/src/main/scala/com/oracle/dicom/connectivity/spark/util/DataAccessParams.scala:47`
  — `val SKIP_OOS_STAGING = "skip.oos.staging"`
- `WriteOperationConfigBuilder.scala:290` — read per write request
  from the writer's options dict.

Setting it on every system-table write is the right behaviour for
qualifire — the system table is always small, always
direct-to-database, never a candidate for OOS staging. Operators
shouldn't have to remember to set it per-deployment; it belongs in
the `external_catalog` backend itself.

### 2. System table creation isn't pre-flighted; init failures get persisted *into the missing table*

`ExternalCatalogStorage.initialize()` runs a best-effort `CREATE
SCHEMA IF NOT EXISTS` then a best-effort `DESCRIBE TABLE` for column
migration. Both are wrapped in try/except and downgrade to
warnings. If the schema or table doesn't exist, the *next*
`write_results` call is the one that errors out — but the engine's
"persist data, then notifications" flow tries to write the engine
warning row about that very failure to the same missing table.
Operator-visible result: a confusing
`[TABLE_OR_VIEW_NOT_FOUND] vectordb23ai.admin.qualifire_history`
exception that *also* surfaces as an attempted self-write to the
table that doesn't exist. The chain reads as if the table both
exists and doesn't.

A related, intermittent failure mode reinforces the same fix:
**implicit create-on-first-`saveAsTable` is not durable on every
external catalog**. Operators report "first write succeeds,
subsequent writes complain table is not present" against
AIDP-style catalogs. The likely causes:

1. **Catalog-cache vs metastore split.** Spark's `SessionCatalog`
   updates its in-memory cache on first `saveAsTable`, but the
   metastore commit can be deferred or batched on certain catalog
   implementations. A second write through a different cache path
   (post-restart, different driver session, replicated metastore
   reader) sees "no such table".
2. **Hive-serde vs data-source-table tagging.** `saveAsTable`
   creates the table as a Spark data-source table. Some external
   catalogs (AIDP, Unity-with-Iceberg, REST catalogs) accept the
   create but persist a non-data-source table. The next
   `saveAsTable("append")` resolves through the data-source
   provider lookup, which fails with table-not-found.
3. **Empty-on-create then write semantics.** If the first append
   writes zero rows, some catalogs skip the create entirely.
4. **Schema-drift between create and append.** A schema mismatch
   between the implicit-on-create shape and the next append's
   DataFrame can be rejected as "not found" rather than as a
   schema error.

Issuing `CREATE TABLE IF NOT EXISTS catalog.schema.table (cols)`
through `spark.sql()` during `initialize()` sidesteps every one
of these — the catalog interprets DDL directly and persists the
table before any data-write path runs. Every subsequent
`saveAsTable("append")` then sees a pre-existing data-source
table.

The fix is to **create the table eagerly during `initialize()`**
via an explicit `CREATE TABLE` DDL — not via the DataFrame writer
or `saveAsTable`. `saveAsTable` on an empty DataFrame *does*
create the table, but it goes through Spark's data-source path
(format inference, write_options interaction with the catalog,
implicit warehouse-path rules, and on AIDP-style catalogs an
unwanted object-storage staging hop). DDL is one short SQL
statement that the catalog interprets directly:

```python
col_defs = ", ".join(
    f"{name} {COLUMN_DEFINITIONS[name][0]}"
    for name in SYSTEM_TABLE_COLUMNS
)
self._spark.sql(
    f"CREATE TABLE IF NOT EXISTS {self._table} ({col_defs})"
)
```

Critical: **no `USING` clause, no `LOCATION` clause, no
`TBLPROPERTIES`.** Verified against today's
`external_catalog.py`: the existing `saveAsTable` call also
provides none of these (`df.write.mode("append").saveAsTable(...)`
is unparameterised), so the migration to DDL is contract-preserving
on that axis. The catalog decides storage format and physical
location. Specifying `LOCATION` would force the table to a
caller-chosen path — wrong for catalog-managed tables and
specifically wrong for in-database catalogs (AIDP / Oracle 23ai)
where the data shouldn't live on object storage at all.

`IF NOT EXISTS` makes the call idempotent. Schema typing comes
from the same `COLUMN_DEFINITIONS` source-of-truth that
`_ensure_columns` and `write_results` already use, so the
declared TIMESTAMP / DOUBLE / STRING shapes are consistent across
create / migrate / write paths.

### 3. `CREATE SCHEMA IF NOT EXISTS` is decorative on most catalogs

Catalogs in the AIDP / Oracle 23ai family don't honour
`CREATE SCHEMA IF NOT EXISTS` from a Spark session — the call
either returns success without doing anything, or errors with a
catalog-specific permission message. Either way, the operator's
real workflow is "ask a DBA to create the schema once, then run
qualifire". The current best-effort wrapper hides whether the call
actually did anything; it can mislead operators into thinking the
schema was provisioned automatically.

The fix is to **drop the `CREATE SCHEMA` call entirely** and
instead **fail loud at `initialize()` time** when the schema
doesn't resolve. Operators get a clear "schema not found, create
it out-of-band and re-run" error at startup rather than a
mid-run failure later.

### 4. Notifications fire on **qualifire-internal** validator exceptions

When a validator's code throws an unhandled exception during
execution (a real-world example from this session: pattern check
hitting `Cannot interpret 'dtype('O')' as a data type` on
Arrow-backed pandas extension dtypes), the engine catches it
here:

```python
# qualifire/core/engine.py:533-543 (and again at ~793-806)
except Exception as e:
    logger.error("Validation %s failed: %s", val_configs[idx].type, e)
    pipeline_map[idx] = PipelineResult(
        validation_results=[ValidationResult(
            validation_name=f"{val_configs[idx].type}_error",
            validation_type=val_configs[idx].type,
            severity=Severity.ERROR,
            message=f"Validation execution error: {e}",
            ...
        )],
    )
```

The synthesised row gets `severity=ERROR` and flows through the
normal persistence + notification routing — so operators get
**paged for what is actually a qualifire-internal bug**, not a
data-quality finding. The downstream alert reads as if the
operator's data is broken when in fact a library invariant was
violated.

This is the same conceptual class as the persistence-infrastructure
suppression below (item #5): engine-level "is this a real
data-quality finding, or a qualifire-internal failure?"
classification. They share the suppression mechanism.

> **Note on the specific dtype('O') symptom.** That root cause
> (extension-dtype handling in `qualifire/validation/_encoding.py`)
> was fixed directly in the same session this feature was
> captured — see `to_numpy(dtype=np.float64)` substitutions plus
> the final-guard `astype(np.float64)` and the new regression
> test `test_extension_dtypes_produce_float_matrix_not_object`.
> The classification work here prevents the *class* of
> qualifire-internal exceptions from triggering notifications
> going forward, even when the next root-cause we hit is a bug
> we haven't found yet.

### 5. Notifications fire even when the system-table layer is broken

The engine's run loop (`qualifire/core/engine.py:_run`) does:

1. `persistence_failed = self._persist_data_rows(result)`
2. `result.notifications = self._send_grouped_notifications(result, ...)`
3. `if persistence_failed: self._best_effort_persist_engine_warning(result)`
4. `self._persist_notification_rows(result)`

So **notifications go out regardless of whether persistence
succeeded**. When the issue is a system-table connectivity / auth /
namespace problem — i.e. an *infrastructure* failure rather than a
data-quality finding — paging Slack / email about the
data-quality results is misleading. Operators want the run to fail
loudly without firing notifications, because the validation
results may be incomplete or unreliable.

The fix is to **bail before notifying when persistence reports an
infrastructure-class failure**. Distinguish "persistence failed
because of system-table issues" from "persistence failed because
one row is malformed" — the former should suppress notifications;
the latter is the existing `engine_warning` path and notifications
remain useful.

### 6. Callers can't programmatically distinguish data-quality findings from library/infra failures

Both today raise the same exception class (`QualifireValidationError`).
Operators wanting to route data-team alerts vs engineering
on-call to different queues have to walk
`e.result.datasets[*].validation_results[*].details` for the
internal-failure marker — plumbing, not API. Industry peers
solve this either by:

- **Returning a result object instead of raising** (Great
  Expectations, Deequ, Soda — data findings are first-class
  return values; only library/infra errors raise). Bigger API
  contract change for qualifire (operators rely on raise-on-ERROR
  for CI/CD non-zero-exit-code monitoring).
- **Typed exception subclasses** (dbt — `DatabaseError` /
  `CompilationException` / `RuntimeException`). Same idiom fits
  qualifire's existing raise-on-ERROR contract cleanly and
  preserves CI/CD semantics.

Adopting the dbt-style typed-exception pattern is the immediate,
back-compat-friendly fix. The bigger GE/Deequ-style return-
result-by-default API revisit is captured separately for a
future feature.

## Why this matters

- **AIDP / 23ai onboarding is the most common new-deployment path.**
  Every issue above bites operators during their first qualifire
  setup against an external catalog. The cumulative effect is
  that the framework looks unreliable on its first encounter
  with the operator.
- **Misleading error chains cost incident time.** The "table not
  found, but the error is in the table" loop in #2 is genuinely
  hard to diagnose without reading the code.
- **False-positive Slack alerts erode trust** in the alerting
  channel. #4 means an outage of the system-table backend pages
  on-call about data-quality findings that may not even be
  computed correctly.

## Affected Areas

- `qualifire/storage/external_catalog.py` — `initialize()`
  rewrite (eager table creation via typed empty DataFrame +
  `mode("append")` + `skip.oos.staging=true`; drop the
  `CREATE SCHEMA IF NOT EXISTS` call; raise
  `QualifireSystemTableError` on missing-schema). `write_results`
  also sets `skip.oos.staging=true` on every append.
- `qualifire/core/engine.py` — two related changes on the
  failure-classification path:
  - Distinguish infrastructure persistence failure from
    row-level persistence warning; short-circuit notifications
    when the former fires. `_persist_data_rows` returns a richer
    reason than just `bool` (an enum / structured outcome
    object).
  - Tag synthesised `ValidationResult`s emerging from the
    validator-execution `except Exception` blocks
    (`engine.py:533-543` and `~793-806`) with an internal-failure
    marker; teach the notification dispatcher to skip notifiers
    for those rows. Both call-sites share one helper so the
    marker shape stays consistent.
- `qualifire/notification/base.py` (or wherever the dispatcher
  lives) — short-circuit on the internal-failure marker before
  invoking notifiers. Add a small docstring + comment so future
  validators that emit ERROR rows know they must NOT set the
  marker for legitimate findings.
- `qualifire/core/exceptions.py` — two new classes:
  - `QualifireSystemTableError` (subclass of
    `QualifireConfigError`) used by `initialize()` for
    fail-loud-on-missing-schema.
  - `QualifireInternalError` (sibling of `QualifireValidationError`,
    both inheriting from `QualifireError`) used by `engine.run()`
    when a run fails entirely due to qualifire-internal issues —
    persistence-infrastructure outage or validator-execution
    exceptions producing the only ERROR rows. Sibling, NOT subclass:
    `except QualifireValidationError` does NOT catch internal
    failures.
- `qualifire/__init__.py` — export the new exception classes
  (`QualifireInternalError`, `QualifireSystemTableError`) at the
  top-level `qualifire` namespace + `__all__` so operators can
  catch them without reaching into `qualifire.core.exceptions`.
- `qualifire/api.py` — docstring updates on
  `Qualifire.run_config()` / `run_config_parsed()` documenting
  both raised exception classes with routing semantics.
- `tests/test_storage/test_external_catalog.py` — coverage for
  eager table creation (idempotent under
  `mode("append")`), missing-schema fail-loud,
  `skip.oos.staging` option set on initialize + every write.
- `tests/test_engine.py` — two suppression sites covered:
  - Notification suppression when `_persist_data_rows` reports
    an infrastructure failure.
  - Notification suppression when a validator throws an
    unhandled exception during execution (synthesised
    "Validation execution error: …" row carries the
    internal-failure marker; configured `error:` channels do
    not fire). Test exercises both engine call-sites
    (sequential at line 793 and parallel-pipeline at line 533).
- `docs/configuration.md` (and `docs/system_table.md` if it
  exists; new section otherwise) — document the eager-create
  contract, the fail-loud-on-missing-schema behaviour, and the
  hard-coded `skip.oos.staging=true` write option.

## Captured Constraints (decision pins for the plan)

1. **System table is created eagerly via `CREATE TABLE IF NOT
   EXISTS` DDL — not `saveAsTable`.** Inside `initialize()` we
   issue a single SQL statement of the form
   `CREATE TABLE IF NOT EXISTS <catalog.schema.table> (col1 type1,
   …)`, with no `USING`, no `LOCATION`, no `TBLPROPERTIES`.
   Column names + types come from `COLUMN_DEFINITIONS` (the same
   source-of-truth `_ensure_columns` and `write_results` use), so
   the declared TIMESTAMP / DOUBLE / STRING shapes stay consistent
   across create / migrate / write. The catalog decides physical
   format and location — qualifire stays neutral about catalog
   implementation. After this call returns, the table exists or
   `initialize()` raised — no deferred-creation surprise mid-run.
2. **`CREATE SCHEMA IF NOT EXISTS` is removed, not made
   best-effort.** Operators on every supported external catalog
   provision the schema out-of-band. Qualifire raises a
   structured `QualifireSystemTableError` when the schema doesn't
   resolve, with the schema name and the catalog-specific
   "ask your DBA" remediation.
3. **`skip.oos.staging=true` is hard-coded inside
   `ExternalCatalogStorage` — not a user-facing config.** The
   system table is always small, always direct-to-database under
   this backend; there is no scenario where qualifire's system
   table benefits from OOS staging. Adding a config knob would
   be footgun-shaped: operators would have to know to enable it.
   Set it unconditionally in both `initialize()` (eager
   create-table) and `write_results` (every append). On
   non-AIDP catalogs that don't recognise the option, Spark
   silently ignores unknown writer options — no harm done; the
   option only takes effect on AIDP-style catalogs that look
   for it.
4. **Qualifire-internal failures suppress notifications across
   two engine paths.** One classification, two enforcement sites:

   **(a) Persistence infrastructure failures.**
   `_persist_data_rows` returns a tagged result
   (`PersistenceOutcome.OK / ROW_LEVEL_WARNING / INFRA_FAILURE`).
   `INFRA_FAILURE` skips the notifier loop and raises the
   underlying exception. `ROW_LEVEL_WARNING` keeps the existing
   `engine_warning` + notifications behaviour. `OK` continues
   unchanged.

   **(b) Validator-execution exceptions.** The two engine
   call-sites that wrap validator-thrown exceptions
   (`qualifire/core/engine.py:533-543` and `~793-806`) currently
   produce a synthesised
   `ValidationResult(severity=ERROR, validation_name=
   '<type>_error', message='Validation execution error: …')`
   that flows through normal notification routing. Change the
   synthesised row to carry an internal-failure marker (e.g.
   `details['qualifire_internal_failure'] = True` or a new
   `Severity.QUALIFIRE_ERROR` enum member — plan picks the
   shape) so the notification dispatcher can short-circuit on it
   without firing the configured `error:` channels. The row is
   still **persisted** (operators need a forensic trail and the
   dashboard surfaces them under a distinct icon), and the
   underlying exception is re-raised at the end of the run so
   the run still fails — just without paging on-call.

   Both paths share the dispatcher logic: "if a row's
   internal-failure marker is set, do not invoke its configured
   notifiers." Documented under a single section in
   `docs/notifications.md` so operators reading either side
   land on the same contract.
5. **Eager-creation is gated to backends that support it.**
   SQLite already creates tables eagerly (`_create_table_if_not_exists`).
   This change applies to `external_catalog`. JDBC and Delta need
   their own per-backend eager-creation pass; tracked as
   follow-up if not landed in this feature.
6. **No silent retry on infrastructure failure.** If the system
   table is unreachable at start of run, the run fails fast.
   Background retries / backoff / partial-write handling are out
   of scope — operators retry the whole run.

7. **Distinct sibling exception class for library/infra failures
   (`QualifireInternalError`).** Captured 2026-05-09 after
   discussion comparing peer-library patterns (Great Expectations,
   Deequ, Soda, dbt — see Issue #6 in problem statement):

   ```
   QualifireError
   ├── QualifireValidationError    (data-quality findings)
   ├── QualifireInternalError      (library / infra failures) ← sibling
   ├── QualifireConfigError
   │   └── QualifireSystemTableError
   └── ...
   ```

   - **Sibling, NOT subclass.** `except QualifireValidationError`
     does NOT catch `QualifireInternalError` and vice versa. Both
     inherit from top-level `QualifireError` for catch-all purposes.
   - **Routing rule** in `engine.run()`:
     - Real data-quality findings only → `QualifireValidationError`
       (data-team queue).
     - All ERROR rows are validator-execution-exception or
       persistence-infra-failure marked rows → `QualifireInternalError`
       (engineering on-call queue).
     - Mix of real findings and internal failures → `QualifireValidationError`
       (data findings take precedence; message includes count of
       internal failures so operators routing to engineering can
       introspect via `isinstance(e, QualifireInternalError)` is
       False after the catch).
     - Persistence-infrastructure outage (storage backend
       unreachable) → `QualifireInternalError` with `__cause__`
       chained to the original storage exception (PEP 3134).
   - **CI/CD non-zero-exit-code monitoring is preserved** —
     uncaught `QualifireInternalError` still produces a non-zero
     exit code. The pattern matches dbt's typed-subclass approach;
     the GE/Deequ/Soda return-result-by-default approach is a
     bigger API revisit captured separately for a future feature.
   - **Behaviour change for existing callers**: code using
     `except QualifireValidationError` to catch infra outages or
     pure validator-execution exceptions has to update to
     `except (QualifireValidationError, QualifireInternalError)` or
     `except QualifireError`. Documented in release notes.

## Open Questions (for the plan phase)

- **Q1. (Closed 2026-05-08.)** The option is `skip.oos.staging`,
  set to `"true"` per write request. Sourced from
  `datalake-connectivity` —
  `connectivity/spark-connectors/.../util/DataAccessParams.scala:47`
  and `WriteOperationConfigBuilder.scala:290`. Hard-coded inside
  the external_catalog backend per decision pin #3.
- **Q2. Should fail-loud-on-missing-schema include a CLI doctor /
  preflight subcommand** (`qualifire doctor <yaml>`) that runs
  the same checks without doing a real run? Useful for first-time
  setup; could be deferred to a separate small feature.
- **Q3. Notification suppression on infra failure — should we
  still send a *different* "qualifire is unreachable" alert to a
  configured channel?** The current proposal is "no alerts at
  all"; an alternative is a separate "infra-down" channel
  (configured via a top-level `infra_alerts:` block) that only
  receives system-table connectivity failures. Operator
  preference; lean toward "no alerts" until there's demand.

## Notes

- Captured 2026-05-08 from a debugging session on
  `vectordb23ai.admin.qualifire_history` (Oracle 23ai vector DB
  catalog). The four issues all surfaced in one operator setup
  attempt.
- Pairs naturally with
  [`metrics-backfill-and-soft-delete`](../metrics-backfill-and-soft-delete/idea.md)'s
  S2/S3 follow-up scope, which extends `is_active` honouring to
  the same external_catalog backend. The two features touch
  adjacent code paths but the work is independent — they can land
  in either order.
- Consumers of the new `system_table_options` field will inherit
  it through
  [`qualifire-from-config-factory`](../qualifire-from-config-factory/idea.md)
  via the YAML pass-through; no special-case wiring needed.
