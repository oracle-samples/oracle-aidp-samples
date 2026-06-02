---
started: 2026-05-08
---

# Implementation Plan: External-catalog system table — initialisation, durability, and notification hygiene

## Overview

Five distinct issues — all hitting the same engine + external_catalog
code paths — fixed in one feature so the storage-layer rework and the
engine-side classifier ship together. **Phase 1** (storage-only)
hardens `qualifire/storage/external_catalog.py`. **Phase 2**
(engine-only) adds qualifire-internal-failure classification so two
distinct kinds of internal failures (persistence-infrastructure and
validator-execution exceptions) suppress notifications without
silencing data-quality findings.

End state:
- `vectordb23ai.admin.qualifire_history` and analogous AIDP-style
  catalogs work out-of-the-box without object-storage staging.
- The system table is created via `CREATE TABLE IF NOT EXISTS` DDL
  on `initialize()`, not deferred to first write.
- Missing schema raises a structured `QualifireSystemTableError` at
  `initialize()` time with a clear "ask DBA" remediation.
- Validator-execution exceptions and persistence infra failures
  no longer page on-call; both are still persisted (forensic
  trail) and the run still fails (re-raised).

## Scope Gates / Hard Stops

- **In scope:** `qualifire/storage/external_catalog.py`,
  `qualifire/core/engine.py`, `qualifire/notification/base.py` (or
  wherever the dispatcher lives), `qualifire/core/exceptions.py`,
  storage + engine tests, `docs/configuration.md` and notification
  routing doc.
- **Out of scope:** `is_active` honoured on Delta / ExternalCatalog /
  JDBC reads — that's tracked in
  [`backfill-api-and-wap-target-mode`](../backfill-api-and-wap-target-mode/idea.md)
  sub-feature K. SQLite / Delta / JDBC table-creation paths are
  unchanged.
- **Out of scope:** `qualifire doctor` preflight subcommand
  (Open Question Q2 from idea.md). Surfaces in a follow-up
  feature if there's demand.
- **Hard stop:** if the engine-side classifier requires touching
  `qualifire/core/models.ValidationResult` in a way that conflicts
  with `findings-sweep`'s typed-identity-keys refactor, abort
  Phase 2 and ship Phase 1 alone — the storage hardening stands
  on its own and operators will get the OOS / DDL / fail-loud wins
  immediately.

## Phase 1 — Storage hardening

`qualifire/storage/external_catalog.py` only. No engine touch.

### Step 1 — Drop `CREATE SCHEMA IF NOT EXISTS`; fail loud on missing schema

- [ ] **1.1** Remove the `CREATE SCHEMA IF NOT EXISTS` call at
  `external_catalog.py:115` and the surrounding try/except wrapper
  (`~110-138`).
- [ ] **1.2** Replace with a `DESCRIBE SCHEMA` (or
  `SHOW SCHEMAS IN <catalog>`) probe to verify the namespace
  exists. Raise a new `QualifireSystemTableError` (added in
  Step 2) with a structured message:
  ```
  System-table schema {namespace!r} does not exist.
  Qualifire does not create catalog schemas — ask your catalog
  admin to create it ({catalog} {schema}) once and re-run.
  ```
- [ ] **1.3** Decision: use `DESCRIBE SCHEMA` (one round-trip,
  fails fast) over `SHOW SCHEMAS` (lists all, may be paginated /
  rate-limited on big catalogs). Document the choice.

  **Fallback note (Round-1 review smaller gap):** Spark accepts
  both `DESCRIBE SCHEMA <ns>` and `DESCRIBE NAMESPACE <ns>` —
  same operator, different verbs across versions / DSV2
  catalogs. Use `DESCRIBE NAMESPACE` first; on a parse error
  (older Spark), fall back to `DESCRIBE SCHEMA`. Both fail with a
  clear "namespace not found" / "schema not found" message on a
  missing namespace, which is what we want for the structured
  remediation hint. Skip the probe entirely (let CREATE TABLE
  fail with the catalog's native error) if both verbs fail with
  parse-error before reaching schema resolution — Risk-table
  mitigation already covers this fallback.

**Exit criteria for Step 1:** initializing against a known-missing
schema raises `QualifireSystemTableError` with the namespace name
and the remediation hint. Initializing against an existing schema
proceeds without DDL emission.

### Step 2 — Add `QualifireSystemTableError`

- [ ] **2.1** New exception class in
  `qualifire/core/exceptions.py`, subclass of
  `QualifireConfigError` so existing callers catching the latter
  for "config-time problems" stay correct.
- [ ] **2.2** Add a `__str__` that includes the namespace + table
  + the remediation hint.
- [ ] **2.3** Re-export from `qualifire.core.exceptions` `__all__`.

**Exit criteria:** `from qualifire.core.exceptions import
QualifireSystemTableError` works; instances `isinstance(...,
QualifireConfigError)` is True.

### Step 3 — Probe-then-create: only issue CREATE DDL when the table is genuinely missing

**Round-3 codex review BLOCKER:** unconditionally executing
`CREATE TABLE IF NOT EXISTS` regresses governed AIDP / Unity / Iceberg
deployments where operators run with **insert-only credentials** and
the table is pre-created by a DBA. Their identities lack CREATE
privilege; the DDL fails even when the table already exists.

**Revised contract:**
- Probe existence first via `DESCRIBE TABLE`.
- Only emit `CREATE TABLE IF NOT EXISTS` if the probe says the table
  is missing.
- If the probe succeeds, the table exists — skip CREATE entirely and
  proceed to `_ensure_columns`. Low-privilege operators stay green.
- If CREATE is required and fails with a permission error, raise
  `QualifireSystemTableError` with explicit "ask your DBA to
  pre-create" remediation — a clear failure point rather than a
  silent regression.

- [ ] **3.0** **Probe table existence first** (Round-3 BLOCKER fix,
  refined per Round-4 codex review BLOCKER):

  Don't try to detect "table missing" — vendor-specific error
  strings vary wildly (Spark `TABLE_OR_VIEW_NOT_FOUND`, Oracle
  `ORA-00942: table or view does not exist` — note no "NOT FOUND"
  substring after uppercase, REST DSV2
  `UNSUPPORTED_OPERATION`, etc.). Inverted detection: key on the
  **small, well-defined permission-denied set**, fall through to
  CREATE for everything else.

  ```python
  # Round-5 codex review BLOCKER fix: bare tokens like "DENIED",
  # "INSUFFICIENT", "ACL" over-match — "request denied by gateway",
  # "insufficient resources/memory", and similar transient /
  # resource errors flip the probe into "table exists" and skip
  # CREATE, regressing first-write. Match on explicit phrases or
  # vendor-specific privilege codes only.
  _PERMISSION_ERROR_PHRASES = (
      "PERMISSION DENIED",
      "ACCESS DENIED",
      "ACCESS CONTROL",
      "NOT AUTHORIZED",
      "INSUFFICIENT PRIVILEGE",       # both singular and plural
      "INSUFFICIENT PRIVILEGES",
      "INSUFFICIENT GRANT",
      "INSUFFICIENT GRANTS",
      "ORA-01031",                    # Oracle: insufficient privileges
      "ORA-00942",                    # Oracle: table or view doesn't exist
                                       # — handled by fall-through, not here;
                                       # listed for documentation but NOT in
                                       # the matcher (would flip the wrong way)
      # NOTE: ORA-00942 is intentionally COMMENTED in the matcher
      # below — it indicates the table is missing, not a privilege
      # issue. Don't add it.
  )
  # The matcher uses only the genuinely permission-denoting phrases.
  # Round-6 codex review RISK #1 + #2 fix: substring matching on
  # short tokens (especially vendor codes like "ORA-01031") risks
  # false-positives when SQL echo / nested cause text contains the
  # literal characters in an unrelated context. Use a regex with
  # word boundaries / line-anchors so the codes only match when
  # they're actually error codes, not embedded payload data.
  import re
  _PERMISSION_PATTERN = re.compile(
      r"\b(?:"
      r"PERMISSION\s+DENIED"
      r"|ACCESS\s+DENIED"
      r"|ACCESS\s+CONTROL"
      r"|NOT\s+AUTHORIZED"
      r"|INSUFFICIENT\s+PRIVILEGE[S]?"
      r"|INSUFFICIENT\s+GRANT[S]?"
      r"|ORA-01031"               # \b before ORA-NNNNN avoids matching
                                   # "12-ORA-01031" or "ORA-010312"
      r")\b",
      re.IGNORECASE,
  )

  table_exists = False
  try:
      self._spark.sql(f"DESCRIBE TABLE {self._table}").collect()
      table_exists = True
  except Exception as e:
      msg = str(e)
      if _PERMISSION_PATTERN.search(msg):
          # Restricted DESCRIBE on an existing table — common in
          # governed catalogs where read metadata requires extra
          # grants. Skip CREATE; downstream INSERT will produce
          # the real error if the table truly is unreachable.
          logger.info(
              "DESCRIBE TABLE %s denied (likely permission-restricted "
              "but table exists); skipping CREATE: %s",
              self._table, e,
          )
          table_exists = True
      else:
          # Anything else — Oracle ORA-00942 ("table or view does
          # not exist"), Spark TABLE_OR_VIEW_NOT_FOUND, REST DSV2
          # UNSUPPORTED_OPERATION, ambiguous AnalysisException —
          # falls through to the CREATE attempt. CREATE TABLE IF
          # NOT EXISTS is itself idempotent, so a false "missing"
          # diagnosis is recoverable; a false "exists" would
          # reintroduce the Round-3 first-write-fails regression.
          logger.info(
              "DESCRIBE TABLE %s did not confirm existence (%s); "
              "attempting CREATE TABLE IF NOT EXISTS",
              self._table, e,
          )
          table_exists = False

  if table_exists:
      logger.info("System table %s exists; skipping CREATE", self._table)
  else:
      ...  # Steps 3.1-3.3 below
  ```

  **Why fall-through is safe:** `CREATE TABLE IF NOT EXISTS` is
  itself idempotent on every catalog we target (Spark / AIDP /
  Iceberg / Unity). A false "table missing" diagnosis costs one
  extra round-trip but doesn't break anything. A false "table
  exists" diagnosis (the old "ambiguous → assume exists" branch)
  would skip CREATE on a genuinely missing table and re-introduce
  the first-write-dies regression Round-3 fixed. Asymmetric error
  cost — fall through to CREATE.
- [ ] **3.1** Build column-list string from
  `COLUMN_DEFINITIONS`:
  ```python
  col_defs = ", ".join(
      f"{name} {COLUMN_DEFINITIONS[name][0]}"
      for name in SYSTEM_TABLE_COLUMNS
  )
  ```
  `COLUMN_DEFINITIONS[name][0]` is the generic SQL type
  (`STRING` / `TIMESTAMP` / `DOUBLE` / `BIGINT`). All four
  external-catalog targets (Spark / AIDP / Iceberg / Unity) accept
  these as ANSI types — verify in the unit test.
- [ ] **3.2** Issue:
  ```python
  self._spark.sql(
      f"CREATE TABLE IF NOT EXISTS {self._table} ({col_defs})"
  )
  ```
  - **No `USING` clause.** Catalog decides the format.
  - **No `LOCATION` clause.** Catalog decides the storage path.
  - **No `TBLPROPERTIES`.** Keep DDL minimal so AIDP / Iceberg /
    Unity all accept it.
- [ ] **3.3** Wrap in try/except. On failure raise
  `QualifireSystemTableError` with a remediation hint that
  classifies the failure into one of FOUR buckets so operators
  can route the fix correctly. Round-5 codex review caught two
  classes the prior plan collapsed (read-only catalog + missing
  namespace) into a generic format-rejection message.

  ```python
  # Round-6 codex review RISK fix: same word-boundary anchoring
  # treatment as _PERMISSION_PATTERN to avoid false-positives
  # from SQL echo / nested cause text.
  _READ_ONLY_PATTERN = re.compile(
      r"\b(?:"
      r"READ[-\s_]?ONLY"
      r"|WRITE\s+NOT\s+ALLOWED"
      r"|MODIFICATION\s+NOT\s+ALLOWED"
      r"|TABLE[-\s_]+IS[-\s_]+READ[-\s_]?ONLY"
      r")\b",
      re.IGNORECASE,
  )
  _NAMESPACE_NOT_FOUND_PATTERN = re.compile(
      r"\b(?:"
      r"SCHEMA[-\s_]?NOT[-\s_]?FOUND"
      r"|NAMESPACE[-\s_]?NOT[-\s_]?FOUND"
      r"|DATABASE[-\s_]?NOT[-\s_]?FOUND"
      r"|ORA-00959"                  # Oracle: tablespace doesn't exist
      r"|ORA-04043"                  # Oracle: object doesn't exist
      r")\b",
      re.IGNORECASE,
  )

  try:
      self._spark.sql(
          f"CREATE TABLE IF NOT EXISTS {self._table} ({col_defs})"
      )
  except Exception as e:
      msg = str(e)

      # CLASSIFIER ORDER MATTERS (Round-6 codex review BLOCKER fix):
      # vendor error messages (Glue, Ranger, etc.) routinely surface
      # BOTH a namespace phrase ("DATABASE NOT FOUND") AND a privilege
      # phrase ("INSUFFICIENT PRIVILEGES") in the same exception text.
      # Earlier rounds put privilege first; that grabbed the dual-
      # phrase case and the operator missed the namespace-creation
      # hint. Reorder: namespace-not-found and read-only take
      # precedence over privilege when both phrases are present, since
      # those failures are upstream of privilege (you can't get the
      # privilege error if the namespace itself is missing — the
      # "namespace not found" cause is the actionable one).

      # 1. Namespace doesn't exist — highest priority (Round-5 RISK fix
      #    + Round-6 BLOCKER ordering).
      if _NAMESPACE_NOT_FOUND_PATTERN.search(msg):
          raise QualifireSystemTableError(
              f"Namespace {namespace!r} does not exist. Qualifire "
              f"does not create catalog schemas — ask your DBA to "
              f"create {namespace!r} once and re-run."
          ) from e

      # 2. Catalog is read-only (Round-5 codex review RISK fix).
      if _READ_ONLY_PATTERN.search(msg):
          raise QualifireSystemTableError(
              f"System table {self._table!r} does not exist and the "
              f"catalog is read-only. Switch the run to a writable "
              f"catalog or ask your DBA to pre-create the table on "
              f"a writable mirror. Required schema: ({col_defs})"
          ) from e

      # 3. Privilege / permission denied during CREATE — last of the
      #    specific buckets so dual-phrase errors get routed to the
      #    upstream cause first.
      if _PERMISSION_PATTERN.search(msg):
          raise QualifireSystemTableError(
              f"System table {self._table!r} does not exist and "
              f"qualifire lacks CREATE privilege on {namespace!r}. "
              f"Ask your DBA to pre-create the table — qualifire "
              f"only needs INSERT privilege from then on. Required "
              f"schema: ({col_defs})"
          ) from e

      # 4. Format-rejection / generic catch-all.
      raise QualifireSystemTableError(
          f"CREATE TABLE on {self._table!r} failed: {e}. Verify the "
          f"catalog accepts `CREATE TABLE IF NOT EXISTS <3-part-name>"
          f" (...)` DDL with no `USING`/`LOCATION`. AIDP / Iceberg / "
          f"Unity all do."
      ) from e
  ```
  Don't swallow exceptions. Four distinct remediation hints in
  precedence order — namespace > read-only > privilege > format —
  so dual-phrase errors route to the upstream cause first.
- [ ] **3.4** Run `_ensure_columns()` AFTER the create. With the
  table now guaranteed to exist, the DESCRIBE-then-ALTER path
  becomes a true column-drift migration rather than a fresh-install
  no-op.

  **Asymmetry pinned (Round-1 review BLOCKER #1):** the schema and
  table existence checks fail loud (Steps 1 + 3.3), but
  `_ensure_columns`'s DESCRIBE-then-ALTER stays best-effort —
  catalogs that reject DESCRIBE on existing tables (some
  governed Iceberg modes, REST catalogs without schema endpoint)
  must continue to work for the unchanged-column-set case. The
  existing log-warning fallback at `external_catalog.py:158-167`
  is preserved verbatim. Document this asymmetry directly in the
  initialize() docstring so operators reading the source see the
  three-tier contract (schema = fail loud / table = fail loud /
  column-drift = best-effort).

**Exit criteria for Step 3:** `initialize()` creates the table
when missing and is a no-op when present (idempotent under
`IF NOT EXISTS`). A subsequent `write_results` call against a
fresh schema succeeds without relying on `saveAsTable`'s
implicit-create path.

### Step 4 — `skip.oos.staging=true` on every external-catalog write

Sourced from `datalake-connectivity` —
`connectivity/spark-connectors/.../util/DataAccessParams.scala:47` +
`builders/WriteOperationConfigBuilder.scala:290`. The option is
honoured per write request by AIDP-style connectors; non-AIDP
catalogs silently ignore unknown writer options.

- [ ] **4.1** Round-3 codex review BLOCKER co-fix +
  Round-2 RISK #5 — switch from `saveAsTable` to `insertInto`
  AND use `.options(**dict)` for write-options:
  ```python
  (df.write.mode("append")
     .options(**_SYSTEM_TABLE_WRITE_OPTIONS)
     .insertInto(self._table))
  ```

  **Why `insertInto` over `saveAsTable`** (matches AIDP user
  guide examples and the operator-confirmed working shape):
  - Schema-strict: rejects column-shape drift at write time
    (the right behaviour for the system table — silent column
    addition / mismatch would corrupt history).
  - Doesn't update table metadata: insert-only credentials work.
  - Doesn't auto-create the table — but Step 3 already
    guaranteed the table exists (probe-then-create), so the
    pre-condition is satisfied.
  - The AIDP-confirmed write shape:
    `df.write.option("skip.oos.staging", "true").mode("append")
    .insertInto("catalog.schema.table")`.

  Today `_SYSTEM_TABLE_WRITE_OPTIONS = {"skip.oos.staging": "true"}`
  is a one-key dict, but the call shape survives any future
  option additions without touching this site again.

  Note: `df` schema must be an exact match for the table's
  column order (`insertInto` is positional, not name-based).
  The existing `_system_table_spark_schema()` builds the
  StructType from `SYSTEM_TABLE_COLUMNS` order — same source
  of truth used by Step 3's CREATE DDL — so positional match is
  guaranteed by construction.
- [ ] **4.2** Round-1 review RISK #1 — pinned. `skip.oos.staging`
  applies to *DataFrame writes*; SQL DDL has no per-statement
  options shape. Spark's `spark.sql(\"CREATE TABLE …\")` does not
  thread `option(...)`-style key-values to the catalog at the
  per-statement level. AIDP's `WriteOperationConfigBuilder.scala:290`
  reads from per-write `options` — irrelevant to DDL because
  DDL has no data-write payload. Conclusion: set the option only
  on `df.write.mode("append").saveAsTable(...)` calls. The
  one-time CREATE TABLE doesn't need it.
- [ ] **4.3** Module-level constant:
  ```python
  _SYSTEM_TABLE_WRITE_OPTIONS = {"skip.oos.staging": "true"}
  ```
  so future writers inherit the same options dict.

**Exit criteria:** every system-table write call site carries
`skip.oos.staging=true`. Verified by a unit test that mocks the
DataFrame writer and asserts the option was set.

## Phase 2 — Engine internal-failure classification

`qualifire/core/engine.py` + `qualifire/notification/base.py`.

### Step 5 — `PersistenceOutcome` enum + `_persist_data_rows` return type

- [ ] **5.1** New `PersistenceOutcomeKind` enum + `PersistenceOutcome`
  dataclass in `qualifire/core/engine.py` (or
  `qualifire/storage/base.py` — pick one and stick with it).
  Final shape pinned in Step 5.3 below; the enum + dataclass
  combo replaces the bare `bool` return type of today's
  `_persist_data_rows`.
- [ ] **5.2** Refactor `_persist_data_rows` (`engine.py:1513-1539`):
  ```python
  def _persist_data_rows(self, result) -> PersistenceOutcome:
      if not self.storage:
          return PersistenceOutcome(kind=PersistenceOutcomeKind.OK)
      rows = self._build_validation_and_collection_rows(result)
      if not rows:
          return PersistenceOutcome(kind=PersistenceOutcomeKind.OK)
      try:
          self.storage.write_results(rows)
          return PersistenceOutcome(kind=PersistenceOutcomeKind.OK)
      except Exception as e:
          logger.error("Failed to persist data rows: %s", e)
          result.engine_warnings.append(ValidationResult(
              validation_name="qualifire.persistence",
              ...
              details={"error": str(e), "qualifire_internal_failure": True},
          ))
          return PersistenceOutcome(
              kind=PersistenceOutcomeKind.INFRA_FAILURE,
              exc=e,
          )
  ```
  Persistence-row-level rejection (some rows accepted, some not)
  is `ROW_LEVEL_WARNING` — no current call site emits that, so
  the enum value is documented future-use; the existing all-or-
  nothing path collapses cleanly to `OK` / `INFRA_FAILURE`.
- [ ] **5.3** Round-1 review BLOCKER #2 — pin the exact shape so the
  re-raise path is unambiguous. Make `PersistenceOutcome` a small
  dataclass instead of a bare enum:
  ```python
  class PersistenceOutcomeKind(Enum):
      OK = "ok"
      ROW_LEVEL_WARNING = "row_warning"
      INFRA_FAILURE = "infra_failure"

  @dataclass(frozen=True)
  class PersistenceOutcome:
      kind: PersistenceOutcomeKind
      exc: Exception | None = None   # populated only for INFRA_FAILURE
  ```
  No singleton. Call sites construct
  `PersistenceOutcome(kind=PersistenceOutcomeKind.OK)` directly
  — frozen dataclasses are cheap and `is`-comparison is never
  needed; everything reads `outcome.kind`. (Round-2 review
  smaller gap — `OK_SINGLETON` would invite accidental
  `is`-comparisons that silently break if a fresh instance is
  constructed elsewhere.)
  `_persist_data_rows` captures the underlying exception in
  `outcome.exc`; the engine main loop re-raises *after*
  `_persist_notification_rows`:
  ```python
  outcome = self._persist_data_rows(result)
  if outcome.kind != PersistenceOutcomeKind.INFRA_FAILURE:
      result.notifications = self._send_grouped_notifications(
          result, snapshot=snapshot,
      )
  else:
      self._best_effort_persist_engine_warning(result)
  self._persist_notification_rows(result)
  if outcome.kind == PersistenceOutcomeKind.INFRA_FAILURE:
      # Round-3 codex review RISK fix: surface data-quality
      # findings in the same exception so operators don't lose
      # them when persistence ALSO fails. The findings are
      # otherwise un-persisted (storage broke) and skipped from
      # notifications (intentional — no false-positive paging on
      # infra failure), so the raised exception is the operator's
      # only signal that the validation side had findings too.
      data_errors = [
          vr for ds in result.datasets
          for vr in ds.validation_results
          if vr.severity == Severity.ERROR
          and not _is_qualifire_internal_failure(vr)
      ]
      if data_errors:
          # Round-6 codex review BLOCKER fix: custom validators and
          # legacy helpers occasionally emit ValidationResult with
          # validation_base_name=None. Naive concatenation would
          # render "None(ERROR)" in the operator-facing exception.
          # Fall back through base_name → validation_name → "<unknown>"
          # so the message is always readable.
          def _name_for_summary(vr) -> str:
              return (
                  getattr(vr, "validation_base_name", None)
                  or getattr(vr, "validation_name", None)
                  or "<unknown>"
              )
          summary = ", ".join(
              f"{_name_for_summary(vr)}({vr.severity.value})"
              for vr in data_errors[:5]
          )
          extra = f" + {len(data_errors) - 5} more" if len(data_errors) > 5 else ""
          msg = (
              f"System-table persistence failed for run {result.run_id} "
              f"AND {len(data_errors)} data-quality ERROR finding(s) "
              f"could not be delivered: [{summary}{extra}]. "
              f"Findings were NOT persisted (storage broken) and NOT "
              f"notified (infra-failure suppression)."
          )
      else:
          msg = (
              f"System-table persistence failed for run {result.run_id}; "
              f"no data-quality ERROR findings to deliver."
          )
      # Persistence-infra failure → QualifireInternalError (sibling
      # of QualifireValidationError, NOT subclass). Engineering
      # on-call queue, not data-team. Idea decision pin #7.
      raise QualifireInternalError(msg, result=result) from outcome.exc

  # Classify the run's ERROR-severity rows: real data-quality findings
  # vs qualifire-internal failures (validator-execution exceptions
  # tagged with details['qualifire_internal_failure']=True).
  if result.has_errors:
      data_errors = [
          vr for ds in result.datasets
          for vr in ds.validation_results
          if vr.severity == Severity.ERROR
          and not _is_qualifire_internal_failure(vr)
      ]
      internal_errors = [
          vr for ds in result.datasets
          for vr in ds.validation_results
          if vr.severity == Severity.ERROR
          and _is_qualifire_internal_failure(vr)
      ]
      if internal_errors and not data_errors:
          # All ERRORs are qualifire-internal. Engineering on-call.
          raise QualifireInternalError(
              f"Run {result.run_id} aborted due to qualifire-internal "
              f"errors ({len(internal_errors)} synthesised failure "
              f"row(s); no data-quality findings). …",
              result=result,
          )
      # Real data findings exist → QualifireValidationError (data
      # team). Mixed runs include count of internal failures in the
      # message so callers routing to engineering can introspect via
      # isinstance check post-catch.
      mixed_note = (
          f" ({len(internal_errors)} qualifire-internal failure(s) "
          f"alongside)" if internal_errors else ""
      )
      raise QualifireValidationError(
          f"Validation failed with ERROR severity for run "
          f"{result.run_id}{mixed_note}",
          result=result,
      )
  ```
  - **Exception class — TWO sibling classes** (Round-1 BLOCKER #2
    pinned shape, refined post-implementation per idea decision
    pin #7):
    - `QualifireValidationError` — data-quality findings
      (existing class; data-team queue).
    - `QualifireInternalError` — library / infra failures (NEW
      sibling; engineering on-call queue). Inherits from
      `QualifireError` directly, NOT from
      `QualifireValidationError`. `except QualifireValidationError`
      does NOT catch `QualifireInternalError`.
    - Both classes carry `result: QualifireResult | None`. CI/CD
      non-zero-exit-code monitoring is unaffected — uncaught
      exceptions still exit non-zero regardless of class.
    - The structured failure-mode information is the message
      prefix + the `__cause__` (PEP 3134 chaining via `raise … from`)
      preserving the original `pyspark.sql.AnalysisException` /
      `py4j.Py4JJavaError` / whatever the catalog raised.
  - **Public exports:** `QualifireInternalError` and
    `QualifireSystemTableError` exported from
    `qualifire/__init__.py` (top-level namespace + `__all__`) so
    operators can `from qualifire import QualifireInternalError`
    without reaching into `qualifire.core.exceptions`.
  - **Order of operations:** notifications and notification-row
    persistence happen *before* the re-raise so any pre-failure
    findings still get audited (`_persist_notification_rows`
    silently no-ops when `result.notifications` is empty, which
    is the INFRA_FAILURE case).
  - **`notify_on_infra_failure=True` opt-in** for "still send
    infra-down alert to a configured channel" is **explicitly
    out of scope** here (Open Question Q3 punted to a follow-up
    feature). Default = no notifications on infra failure;
    no API surface for the opt-in yet.

**Exit criteria for Step 5:** with a deliberately broken storage
(mock raising on `write_results`), the run produces zero
notifications, persists the engine_warning row via best-effort
retry, and re-raises the underlying exception.

### Step 6 — Tag validator-execution exceptions with internal-failure marker

- [ ] **6.1** Single shared helper at top of `engine.py`:
  ```python
  def _validator_execution_error_result(val_config, ds_config, exc) -> ValidationResult:
      """Wrap an unhandled exception from validator code as a
      ValidationResult marked as qualifire-internal so the
      notification dispatcher skips it."""
      return ValidationResult(
          validation_name=f"{val_config.type}_error",
          validation_type=val_config.type,
          severity=Severity.ERROR,
          message=f"Validation execution error: {exc}",
          validation_base_name=_resolve_validation_base_name(val_config, ds_config),
          details={"qualifire_internal_failure": True, "error": str(exc)},
      )
  ```
- [ ] **6.2** Replace both call sites:
  - `engine.py:535-542` (parallel-pipeline branch)
  - `engine.py:798-806` (sequential branch)
  with a call to the helper. Identical shape across both.
- [ ] **6.3** Same helper for the `engine.run()` parallel-dataset
  outcome path at `engine.py:322-330` (`qualifire.dataset_error`),
  except keep the `validation_name='qualifire.dataset_error'`
  override — this is a different layer (dataset) but the same
  internal-failure semantics. Add `qualifire_internal_failure=True`
  to its `details` too.

  **Round-2 review BLOCKER #3 — nested wrappers acceptable:** if a
  dataset-level error is *caused by* a validator-execution
  exception that already produced a marked row, the system table
  will carry both — one `<type>_error` row from line 533/793 and
  one `qualifire.dataset_error` row from line 322. That's
  intentional: each row records a different layer's view of the
  same root cause; both are filtered out of notifications by the
  shared `_is_qualifire_internal_failure` predicate; the
  dashboard can render them with the distinct internal-failure
  icon. Document this explicitly in the helper's docstring so
  future contributors don't try to "deduplicate" the rows
  thinking it's a bug.

- [ ] **6.4** Round-2 review RISK #4 — distinguish all-internal
  failures in the final raise message. After the run loop,
  before raising, classify the errors:
  ```python
  if result.has_errors:
      all_errors = [vr for ds in result.datasets
                       for vr in ds.validation_results
                       if vr.severity == Severity.ERROR]
      all_internal = (
          all_errors
          and all(_is_qualifire_internal_failure(vr) for vr in all_errors)
      )
      if all_internal:
          msg = (
              f"Run {result.run_id} aborted due to qualifire-internal "
              f"errors ({len(all_errors)} synthesised failure row(s); "
              f"no data-quality findings). Inspect "
              f"result.datasets[*].validation_results[*] for rows "
              f"with details['qualifire_internal_failure']=True."
          )
          # Idea decision pin #7: distinct sibling exception class
          # so engineering on-call routes via
          # `except QualifireInternalError` rather than walking
          # result.datasets for the marker.
          raise QualifireInternalError(msg, result=result)
      mixed_note = (
          f" ({len(internal_errors)} qualifire-internal failure(s) "
          f"alongside)" if internal_errors else ""
      )
      raise QualifireValidationError(
          f"Validation failed with ERROR severity for run "
          f"{result.run_id}{mixed_note}",
          result=result,
      )
  ```
  Two sibling exception classes — `QualifireValidationError` for
  the data-team queue, `QualifireInternalError` for engineering
  on-call. Mixed runs (real findings + internal alongside) raise
  `QualifireValidationError` because the data findings are the
  actionable result; the internal-failure count is in the
  message so post-catch `isinstance(e, QualifireInternalError)`
  introspection can tell engineering. Tests in Step 9 cover all
  three branches (real-only / internal-only / mixed).

**Exit criteria for Step 6:** `git grep "Validation execution
error"` returns only the helper definition + tests.

### Step 7 — Notification dispatcher short-circuits on the marker

The dispatcher at `_send_grouped_notifications`
(`engine.py:1250+`) iterates per-validation routing. Two changes:

- [ ] **7.1** Round-1 review RISK #2 — pinned. The dispatcher has
  TWO sites that read the validation list, and BOTH must filter:
  - **Severity bucketing** — `_send_grouped_notifications`
    (`engine.py:1250+`) iterates per-validation routing to decide
    which channels fire at which severity.
  - **Per-channel message building** — `format_validation_details`
    and `format_grouped_notification_message` in
    `qualifire/notification/base.py` render
    SHAP / drift / threshold details from `vr.details` into the
    Slack/email body. If only the bucketing filters, the message
    body still references the dropped row's findings.

  Single shared helper:
  ```python
  def _is_qualifire_internal_failure(vr) -> bool:
      details = getattr(vr, "details", None) or {}
      return bool(details.get("qualifire_internal_failure"))
  ```
  Imported and applied at both call sites. They still appear in
  the persisted system-table row (via
  `_build_validation_and_collection_rows`) and in the
  `result.has_errors` calculation (so the run still raises), but
  they don't drive notifier dispatch and don't appear in
  notification message bodies.
- [ ] **7.2** Document the rule in
  `qualifire/notification/base.py` near
  `format_validation_details` (where validator-type-specific
  rendering lives) so future contributors know that internal-
  failure rows have a separate dispatch contract.

**Exit criteria for Step 7:** with a validator forced to throw an
unhandled exception (e.g. monkey-patched
`PatternCheckValidator.validate` to `raise RuntimeError("boom")`),
the run produces zero notifications, persists the synthesised
`qualifire.<type>_error` row, and re-raises
`QualifireValidationError` as today.

## Phase 3 — Tests + docs

### Step 8 — Storage tests

**Test fixture style (Round-1 review smaller gap, corrected
post-grep):** the actual external_catalog tests live in
`tests/test_storage/test_spark_storages.py` under
`class TestExternalCatalogStorage`. They use `unittest.mock.MagicMock`
for the Spark session plus `MockDataFrame` / `MockRow` from
`tests.conftest`. Reuse that pattern — extend the existing class
with new tests, don't introduce a parallel file.

**Existing tests requiring contract inversion** (Phase 1
inverts the contract that `initialize()` does NOT create the
table):
- `test_initialize_creates_namespace_not_table`
  (`test_spark_storages.py:24`) — invert: rename to
  `test_initialize_creates_table_via_ddl` and assert the
  `CREATE TABLE IF NOT EXISTS` SQL is fired.
- `test_initialize_tolerates_create_schema_failure`
  (`test_spark_storages.py:152`) — invert: rename to
  `test_initialize_raises_on_missing_schema` and assert
  `QualifireSystemTableError` is raised. The "tolerates"
  semantic is gone.

- [ ] **8.1** New tests added to `class TestExternalCatalogStorage`:
  - `test_initialize_creates_table_idempotent` — fresh catalog,
    verify `CREATE TABLE` ran. Second call is a no-op.
  - `test_initialize_raises_on_missing_schema` — schema absent,
    verify `QualifireSystemTableError` with namespace name + hint.
  - `test_initialize_skip_oos_staging_set_on_writes` — assert
    every `write_results` call carries `skip.oos.staging=true`.
  - `test_create_table_uses_no_using_no_location` — capture the
    issued SQL string and assert it matches
    `CREATE TABLE IF NOT EXISTS … (col1 type1, …)` exactly,
    no `USING`, no `LOCATION`, no `TBLPROPERTIES`.
  - `test_no_create_schema_calls_in_source` (Round-2 review
    RISK #6 — the durable replacement for the brittle
    `git grep`): reads
    `qualifire/storage/external_catalog.py` as text and asserts
    `"CREATE SCHEMA"` substring is absent. Cheap, fast, and
    survives future renames better than a CI grep.
  - `test_initialize_skips_create_when_table_exists` (Round-3
    codex review test gap): mock the Spark session so
    `DESCRIBE TABLE` succeeds; assert `CREATE TABLE` SQL is
    NOT issued. The low-privilege operator path — DBA pre-
    created the table, qualifire identity has only INSERT.
  - `test_initialize_create_permission_denied_raises_with_dba_hint`
    (Round-3 codex review test gap): mock `DESCRIBE TABLE` to
    raise "TABLE_OR_VIEW_NOT_FOUND" and `CREATE TABLE` to raise
    a permission-denied error. Assert
    `QualifireSystemTableError` is raised with "Ask your DBA"
    + the required schema in the message. Operators with
    insert-only credentials and a schema not yet pre-created
    get the right remediation.
  - `test_initialize_oracle_table_or_view_does_not_exist_falls_through_to_create`
    (Round-4 codex review test gap): mock `DESCRIBE TABLE` to
    raise an exception whose `str(e)` is
    `"ORA-00942: table or view does not exist"` (the canonical
    Oracle / AIDP error string — note no "NOT FOUND"
    substring). Assert the probe falls through to
    `CREATE TABLE IF NOT EXISTS` and the table is created.
    Pins the inverted detection rule: don't pattern-match on
    "missing", pattern-match on "permission-denied" and let
    everything else attempt CREATE.
  - `test_initialize_unsupported_describe_falls_through_to_create`
    (Round-4 codex review RISK fix): mock `DESCRIBE TABLE` to
    raise `UNSUPPORTED_OPERATION` (REST DSV2 catalogs without
    DESCRIBE support). Assert CREATE TABLE still runs.
  - `test_initialize_describe_permission_denied_skips_create`
    (Round-4 codex review BLOCKER reinforcement): mock
    `DESCRIBE TABLE` to raise an exception whose `str(e)`
    contains an explicit privilege phrase (e.g.
    `"insufficient privileges"` or `"ORA-01031"`). Assert
    CREATE is NOT issued. Low-privilege operator with
    restricted DESCRIBE stays green.
  - `test_initialize_describe_resource_error_falls_through_to_create`
    (Round-5 codex review BLOCKER fix — over-match guard):
    mock `DESCRIBE TABLE` to raise a non-permission exception
    whose `str(e)` *contains* a misleading bare token like
    `"insufficient resources for DESCRIBE"` or
    `"connection denied by gateway"`. Assert CREATE IS still
    attempted (the matcher only fires on whole privilege
    phrases, not bare tokens, so resource errors fall through
    correctly). Pins the tightened phrase-only matcher.
  - `test_initialize_create_read_only_catalog_raises_with_dba_hint`
    (Round-5 codex review RISK fix): mock `CREATE TABLE` to
    raise an exception with `"catalog is read only"` in the
    message. Assert `QualifireSystemTableError` is raised with
    the read-only-mirror remediation, not the format-rejection
    one.
  - `test_initialize_create_namespace_not_found_raises_with_namespace_hint`
    (Round-5 codex review RISK fix): mock `CREATE TABLE` to
    raise an exception with `"SCHEMA_NOT_FOUND"` (or
    `"NAMESPACE NOT FOUND"`). Assert
    `QualifireSystemTableError` is raised with the
    namespace-creation hint pointing at the namespace. This
    covers DESCRIBE-unsupported catalogs where Step 1's probe
    falls through silently.
  - `test_initialize_create_dual_phrase_namespace_plus_privilege`
    (Round-6 codex review BLOCKER fix — classifier order): mock
    `CREATE TABLE` to raise an exception whose message contains
    BOTH "DATABASE NOT FOUND" AND "INSUFFICIENT PRIVILEGES"
    (Glue / Ranger style). Assert the namespace-creation hint
    is raised, NOT the privilege hint. The order is: namespace
    > read-only > privilege > format-rejection.
  - `test_initialize_create_dual_phrase_readonly_plus_privilege`:
    mock to raise both "READ ONLY" AND "PERMISSION DENIED".
    Assert the read-only hint is raised, not the privilege
    hint. Same precedence rule.
  - `test_permission_pattern_does_not_match_embedded_substrings`
    (Round-6 codex review RISK fix — regex word boundaries):
    mock `DESCRIBE TABLE` to raise an exception whose message
    embeds "ORA-01031" inside a longer token like
    `"12-ORA-010312"` or `"NORA-01031Z"`. Assert the matcher
    does NOT fire (table_exists stays False, CREATE is
    attempted). Pins the `\b...\b` anchoring against false-
    positive substring matches in nested SQL echo.
  - `test_write_uses_insert_into_not_save_as_table` (AIDP-
    confirmed write shape): assert the writer's terminal
    method is `insertInto`, NOT `saveAsTable`. The MagicMock
    fixture captures the chained call sequence; assert
    `df.write.mode("append").options(...).insertInto(table)`
    matches the actual call shape.
  - `test_write_carries_skip_oos_staging_option`: assert
    `_SYSTEM_TABLE_WRITE_OPTIONS` makes it onto the writer's
    options dict on every `write_results` call.
- [ ] **8.2** Update / invert existing tests in
  `test_spark_storages.py:TestExternalCatalogStorage`:
  - `test_initialize_creates_namespace_not_table` →
    `test_initialize_creates_table_via_ddl` (assert
    `CREATE TABLE IF NOT EXISTS` SQL fires; assert
    `CREATE SCHEMA IF NOT EXISTS` does NOT fire).
  - `test_initialize_tolerates_create_schema_failure` →
    `test_initialize_raises_on_missing_schema` (assert
    `QualifireSystemTableError` is raised when DESCRIBE
    NAMESPACE / SCHEMA reports missing).
  - `test_write_results` (line 76) — assert the writer was
    called with `.options(skip_oos_staging="true", ...)` (or
    equivalent — the test fixture inspects MagicMock call args
    so we can verify `skip.oos.staging=true` lands on the
    actual options dict passed through).

### Step 9 — Engine tests

- [ ] **9.1** `tests/test_engine_failure_classification.py`:
  - `test_infra_failure_skips_notifier_dispatch` — storage mock
    raises on `write_results`; run produces 0 notifications; the
    engine_warning row is best-effort persisted; the run raises
    `QualifireInternalError` (sibling, NOT `QualifireValidationError`)
    with the original storage exception chained as `__cause__`.
  - `test_infra_failure_persists_engine_warning_via_best_effort_retry`
    — confirms the qualifire.persistence engine_warning carries
    the marker so downstream consumers (dashboards, audit
    queries) can distinguish it.
  - `test_validator_exception_marked_and_skipped_from_notifications`
    — monkey-patched validator raises; synthesised `<type>_error`
    row carries the marker; 0 notifications fire;
    `QualifireInternalError` is raised (no real findings
    alongside; sibling-class assertion pins the contract).
  - `test_validator_exception_row_reaches_persisted_history` —
    synthesised marked row hits storage.write_results for forensic
    trail.
  - **`TestExceptionClassRouting` class** (idea decision pin #7):
    - `test_sibling_classes_not_caught_by_each_other` — pins the
      inheritance contract via `issubclass()` checks both
      directions (neither `QualifireValidationError` nor
      `QualifireInternalError` subclasses the other; both subclass
      top-level `QualifireError`).
    - `test_mixed_findings_and_internal_raises_validation_error`
      — real data finding + internal failure alongside routes to
      `QualifireValidationError`; message includes
      `"qualifire-internal"` substring + count for engineering
      introspection.
  - **All-internal predicate tests** (`TestIsQualifireInternalFailure`):
    - `test_marker_true_dict_payload`,
      `test_marker_false_when_absent`,
      `test_marker_handles_string_payload` (Round-7 codex review
      RISK fix — JSON string from re-read storage),
      `test_marker_handles_malformed_json_string_safely`,
      `test_marker_false_when_value_is_falsy`.
  - **`PersistenceOutcome` shape pinning** (`TestPersistenceOutcomeShape`):
    OK / INFRA_FAILURE constructors; outcome.exc captured;
    dataclass is frozen.

### Step 10 — Reporting / dashboard wiring (Round-6 codex review BLOCKER fix)

The persisted `details['qualifire_internal_failure']=True` marker
needs to surface in the reporting layer so dashboards distinguish
qualifire-internal failures from genuine data-quality findings.
Without this, an operator looking at the dashboard sees a sea of
ERROR rows and can't tell which were paged-on (data findings) vs
suppressed (internal failures).

- [ ] **10.0** Plumb the marker through
  `qualifire/reporting/system_table.py` (or `health.py` /
  whichever path materialises rows for the dashboard) so it
  reaches the rendering layer:
  - `health.py`'s severity counters (lines 49+, 66+, 84+, 106+)
    keep counting marked rows as ERROR for `total_checks` /
    `error_rate` accuracy — but emit a SEPARATE `internal_error`
    counter on `HealthReport` so the dashboard can split the
    two visually.
  - `system_table.py` row materialisation reads
    `details_json` (already a column) and surfaces a
    `qualifire_internal_failure` boolean field on the rendered
    row dict.
  - **Round-7 codex review RISK fix — payload-shape tolerance.**
    `details_json` arrives as either a dict (in-memory engine
    path) or a string (re-read from the persisted system table —
    the column is TEXT). The marker-extraction helper must
    handle both:
    ```python
    def _row_is_internal_failure(row) -> bool:
        details = row.get("details_json")
        if details is None:
            return False
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except (json.JSONDecodeError, ValueError):
                return False
        if not isinstance(details, dict):
            return False
        return bool(details.get("qualifire_internal_failure"))
    ```
    Without this, the string-payload path silently misclassifies
    every persisted internal-failure row as a normal ERROR.
- [ ] **10.1** Update `qualifire/reporting/html_report.py` to
  render marked rows with a distinct visual treatment:
  - Add an "internal error" filter to the dashboard severity
    legend (gray icon vs the existing red ERROR badge).
  - Status-pie / trend-line charts: marked rows excluded from
    the data-quality ERROR bucket and shown in a separate
    `internal-error` series so operators can tell at a glance
    whether ERROR spikes are real findings or library bugs.
  - Dashboard table: add a column or icon column rendering the
    marker as a wrench / gear emoji or similar (the existing
    SHAP-message column doesn't carry the signal).
- [ ] **10.2** Update `tests/test_interactive_dashboard.py`:
  - `test_internal_failure_rows_segregated_in_severity_pie` —
    seed a row with `details_json` containing
    `qualifire_internal_failure=True`; assert it doesn't count
    in the data-quality ERROR bucket.
  - `test_internal_failure_rows_render_with_distinct_icon` —
    snapshot-test the rendered HTML for the icon class.
  - `test_internal_failure_marker_handles_string_and_dict_details_json`
    (Round-7 codex review RISK fix): seed two rows — one with
    `details_json` as a Python dict (in-memory engine path),
    one with `details_json` as a JSON string (re-read from
    persistence). Both must classify as internal failure. Pins
    `_row_is_internal_failure` payload-shape tolerance.

### Step 11 — Docs

- [ ] **11.1** New section in
  `docs/configuration.md` — "External catalog system table
  hardening": eager DDL, fail-loud-on-missing-schema, hardcoded
  `skip.oos.staging=true`, why no `LOCATION` / `USING`.
- [ ] **11.2** New section in `docs/notifications.md` (or
  closest existing doc on notifications routing) — "Notifications
  on qualifire-internal failures": clarifies that
  `qualifire.persistence`, `qualifire.<type>_error`, and
  `qualifire.dataset_error` rows do **not** trigger configured
  notifiers; pointer to the structured `details` marker for
  dashboard authors.

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Some external catalogs reject `CREATE TABLE IF NOT EXISTS` with no `USING` clause | Phase 1 step 3.3 raises `QualifireSystemTableError` with the catalog's underlying error verbatim — operators get a clear failure point and can either fix the catalog config or open an issue. Spark / AIDP / Iceberg / Unity all accept the bare form per documented DDL. |
| Insert-only operator runs against a governed catalog where DBA pre-created the table | Round-3 codex review BLOCKER fix: Step 3.0 probes `DESCRIBE TABLE` first; if the probe succeeds the table exists and CREATE is skipped entirely. Low-privilege identities (no CREATE / no ALTER) are first-class. Test `test_initialize_skips_create_when_table_exists` pins the behaviour. |
| Vendor-specific "table missing" error strings (Oracle `ORA-00942`, REST DSV2 `UNSUPPORTED_OPERATION`, etc.) bypass the probe's substring matcher | Round-4 codex review BLOCKER fix: detection logic is INVERTED — pattern-match on permission-denied phrases and fall through to CREATE for everything else. `CREATE TABLE IF NOT EXISTS` is itself idempotent, so false-missing costs one round-trip; false-exists would reintroduce the regression. Tests `test_initialize_oracle_table_or_view_does_not_exist_falls_through_to_create` and `test_initialize_unsupported_describe_falls_through_to_create` pin both vendor variants. |
| Bare permission-denied tokens (`DENIED`, `INSUFFICIENT`, `ACL`) over-match transient resource errors and gateway failures, flipping `table_exists=True` falsely | Round-5 codex review BLOCKER fix: matcher tightened to explicit phrases only — `PERMISSION DENIED`, `ACCESS DENIED`, `ACCESS CONTROL`, `NOT AUTHORIZED`, `INSUFFICIENT PRIVILEGE(S)`, `INSUFFICIENT GRANT(S)`, `ORA-01031`. No bare-token matches. Test `test_initialize_describe_resource_error_falls_through_to_create` pins resource errors fall through to CREATE. |
| Read-only catalog produces format-rejection error message instead of catalog-misconfiguration hint | Round-5 codex review RISK fix: Step 3.3 has a dedicated `_READ_ONLY_PHRASES` branch raising `QualifireSystemTableError` with read-only-mirror remediation. Test `test_initialize_create_read_only_catalog_raises_with_dba_hint` pins the message routing. |
| DESCRIBE-unsupported catalog skips the schema probe in Step 1, then CREATE raises generic format-rejection error instead of namespace-creation hint | Round-5 codex review RISK fix: Step 3.3 has a dedicated `_NAMESPACE_NOT_FOUND_PATTERN` branch — schema-not-found / namespace-not-found / `ORA-00959` / `ORA-04043` are all classified into the namespace-creation hint message. Test `test_initialize_create_namespace_not_found_raises_with_namespace_hint` pins the routing. |
| Vendor errors (Glue, Ranger) surface BOTH a namespace phrase and a privilege phrase in the same message, with classifier ordering grabbing privilege first and missing the upstream cause | Round-6 codex review BLOCKER fix: classifier order reordered to namespace → read-only → privilege → format. Tests `test_initialize_create_dual_phrase_namespace_plus_privilege` and `test_initialize_create_dual_phrase_readonly_plus_privilege` pin the precedence. |
| `INFRA_FAILURE` exception summary renders `None(ERROR)` when `validation_base_name` is None | Round-6 codex review BLOCKER fix: helper `_name_for_summary(vr)` falls back through `validation_base_name → validation_name → "<unknown>"`. |
| Dashboard renders `qualifire_internal_failure=True` rows as ordinary ERROR, defeating the "distinct icon" contract | Round-6 codex review BLOCKER fix: Step 10 added — `health.py` exposes a separate `internal_error` counter, `html_report.py` filters / renders marked rows distinctly, two new tests in `test_interactive_dashboard.py`. |
| Substring matching on `ORA-01031` / `INSUFFICIENT PRIVILEGES` / etc. matches embedded payload text from nested SQL echo (false-positive flips `table_exists=True`) | Round-6 codex review RISK fix: matchers switched from substring `in` to compiled regex with `\b` word boundaries. Test `test_permission_pattern_does_not_match_embedded_substrings` pins the boundary behaviour. |
| Operator on a green-field schema lacks CREATE privilege | Step 3.3 distinguishes permission-denied from format-rejection in the error message. The DBA-hint variant explicitly tells the operator to ask their DBA to pre-create the table with the listed schema, and notes that qualifire only needs INSERT from then on. |
| `insertInto` positional column order differs from the table's actual order | Both the CREATE DDL (Step 3) and the write DataFrame (Step 4) build their column list from `SYSTEM_TABLE_COLUMNS` order via `_system_table_spark_schema()` / `COLUMN_DEFINITIONS`. Single source of truth — positional alignment is a side-effect, not a runtime check. Test exists to pin the schema shape. |
| Run hits BOTH system-table outage and data-quality ERRORs; data findings get lost | Round-3 codex review RISK fix: Step 5.3's INFRA_FAILURE re-raise summarises up to 5 data-error base names + count in the exception message. Operators reading the raised `QualifireInternalError` see both axes of the failure even though notifications were suppressed. |
| Existing operator code uses `except QualifireValidationError` to catch both data findings and infra outages; the new sibling class breaks that pattern silently | Idea decision pin #7 + post-implementation codex review (round 4) caught this: docs/notifications.md + Qualifire.run_config docstrings document the routing semantics; `QualifireInternalError` exported at top-level `qualifire` namespace + `__all__`. CI/CD non-zero-exit-code monitoring is preserved (uncaught exceptions still exit non-zero). Migration: `except (QualifireValidationError, QualifireInternalError)` or `except QualifireError`. |
| `findings-sweep`'s typed-identity-keys refactor lands on `ValidationResult.details` and conflicts with the marker shape | Use `details["qualifire_internal_failure"]` (string-keyed dict) rather than a new field — survives any reasonable refactor. If `findings-sweep` lands first and changes `details` semantics, rebase. |
| Notification dispatcher refactor breaks existing per-validation routing tests | Step 7 changes only the inclusion predicate, not the routing logic. Existing routing tests' fixtures don't carry the marker, so they continue to fire notifications unchanged. New tests cover the marker case explicitly. |
| `skip.oos.staging` option name changes upstream | Module-level constant `_SYSTEM_TABLE_WRITE_OPTIONS` keeps the source-of-truth in one place; if AIDP renames the option, single-line fix. |
| Catalog refuses `DESCRIBE SCHEMA` (probe in Step 1.2) | Fall back to attempting `CREATE TABLE IF NOT EXISTS` directly and let its error message guide remediation — `CREATE TABLE` on a missing namespace fails with a clear "schema not found" on every catalog tested, so the probe is an optimization not a requirement. |

## Test Plan Summary

- **Extended:** `tests/test_storage/test_spark_storages.py`
  (`class TestExternalCatalogStorage`) gains the new tests
  enumerated under Step 8 (12 tests total covering eager-create,
  inversion contract, dual-phrase classifier, regex word-
  boundaries, AIDP `insertInto` shape, etc.). Round-7 codex
  review smaller-item fix: previously listed as a parallel new
  file `test_external_catalog_init.py` — that would have
  duplicated fixtures. Single home, single class.
- **New:** `tests/test_engine_failure_classification.py`
  (2 tests covering both suppression sites at `engine.py:533/793`
  and `_persist_data_rows` infra-failure path).
- **Updated:** existing external_catalog tests where `CREATE SCHEMA`
  assertions exist (inverted to "no CREATE SCHEMA" + new
  CREATE-TABLE assertions); existing engine tests where
  `_persist_data_rows` return-type was `bool` (now
  `PersistenceOutcome`).
- **Sanity:** full `pytest -q` green; `grep -rn 'CREATE SCHEMA IF
  NOT EXISTS'` returns only docs / migration notes (no live code).

## Implementation Order

Steps 1 → 2 → 3 → 4 (Phase 1, storage-only PR-able alone)
→ 5 → 6 → 7 (Phase 2, engine classification)
→ 8 → 9 → 10 (tests + docs).

If Phase 2 hits the `findings-sweep` conflict (Hard Stop above),
Phase 1 is shippable as a standalone PR with the engine
classification deferred to a follow-up. Phases are independent in
the sense that storage hardening doesn't depend on engine
classification or vice versa; bundling them is for operator-
visible coherence ("the same error class doesn't notify").

**Mid-implementation rollback procedure (Round-1 review smaller
gap):** Phase 1 commits are atomic and don't depend on Phase 2.
If Phase 2 hits the typed-identity-keys conflict mid-implementation,
the rollback is `git reset --hard <last-Phase-1-commit-sha>`;
re-open the PR with Phase 1 only and link a follow-up issue for
Phase 2. Concretely: keep Phase 1 commits clean (Steps 1-4 + their
tests) before any Phase 2 commit lands, so the reset is one
sha-pointer move. Phase 2's commits should sit on top, individually
revertable if any single sub-step (5/6/7) breaks.

## Acceptance Checklist

- [ ] `vectordb23ai.admin.qualifire_history` first run creates the
      table; subsequent runs no-op.
- [ ] Missing schema → `QualifireSystemTableError` at
      `initialize()` with namespace + hint.
- [ ] `git grep -n 'CREATE SCHEMA IF NOT EXISTS'` returns only
      docs / migration notes (no live code). Pinned in code via
      `test_no_create_schema_calls_in_source` in Step 8 — reads
      `qualifire/storage/external_catalog.py` source as text and
      asserts the substring is absent. (Round-2 review RISK #6
      — grep is fragile; the test is durable.)
- [ ] Every external-catalog write carries `skip.oos.staging=true`.
- [ ] `_persist_data_rows` returns `PersistenceOutcome`; the
      engine main loop respects it.
- [ ] Validator-execution exceptions and persistence infra
      failures persist the row + skip notifier dispatch + still
      raise.
- [ ] Marker (`details['qualifire_internal_failure']=True`) is
      visible on the persisted row.
- [ ] **`QualifireInternalError` is a sibling of
      `QualifireValidationError` (NOT subclass).** Both inherit
      from top-level `QualifireError` for catch-all. Pinned by
      `test_sibling_classes_not_caught_by_each_other` in Step 9.
- [ ] **Routing**: real-only findings → `QualifireValidationError`;
      all-internal → `QualifireInternalError`; mixed →
      `QualifireValidationError` with internal count in message;
      persistence-infra → `QualifireInternalError` with
      `__cause__` chained via PEP 3134.
- [ ] **Public exports**: `QualifireInternalError` and
      `QualifireSystemTableError` available at top-level
      `qualifire` namespace + `__all__` so callers can
      `from qualifire import QualifireInternalError`.
- [ ] **API docstrings**: `Qualifire.run_config()` /
      `run_config_parsed()` document both raised exception
      classes with routing semantics (Phase 2 of feature work).
- [ ] Full `pytest -q` green.
- [ ] Docs updated (`docs/configuration.md` +
      `docs/notifications.md` — including the "Distinct exception
      class" section with worked except/raise examples and
      `__cause__` usage pattern).
