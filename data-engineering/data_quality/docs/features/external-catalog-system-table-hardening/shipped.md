---
shipped: 2026-05-09
---

# Shipped: External Catalog System Table Hardening

## Summary
Hardened the external-catalog system-table path so it (a) survives Glue/Ranger
permission edges via probe-then-create, (b) classifies infra/library failures
distinctly from data-quality findings via a sibling exception class, and (c)
surfaces internal failures in dashboards/notifications without poisoning the
operator alert channel or the displayed pass-rate.

## Key Changes

### Storage — `qualifire/storage/external_catalog.py`
- Probe-then-create initialization: `_table_exists_via_describe()` first, then
  `_create_system_table()` only if needed.
- 4-bucket error classifier with explicit precedence (namespace > read-only >
  privilege > format-rejection), backed by module-level `_PERMISSION_PATTERN`,
  `_READ_ONLY_PATTERN`, `_NAMESPACE_NOT_FOUND_PATTERN` regexes that use `\b`
  word boundaries to avoid Oracle ORA-NNNNN substring false-positives.
- Permission-denied on DESCRIBE → assume table exists (Ranger pattern); other
  errors fall through to CREATE.
- Writes use `insertInto` with `_SYSTEM_TABLE_WRITE_OPTIONS = {"skip.oos.staging":
  "true"}` (sourced from datalake-connectivity `WriteOperationConfigBuilder`).

### Exception classes — `qualifire/core/exceptions.py`
- New `QualifireInternalError` as a **sibling** (not subclass) of
  `QualifireValidationError`. Both inherit from `QualifireError` directly.
- Routing contract: `except QualifireValidationError` does NOT catch internal
  failures and vice versa — operators wire data-team alerts and engineering
  on-call to different queues.
- Public exports added to `qualifire/__init__.py`.

### Engine — `qualifire/core/engine.py`
- New `PersistenceOutcomeKind` enum + `PersistenceOutcome` frozen dataclass.
- New `_is_qualifire_internal_failure` predicate (dict + JSON-string payload
  tolerance) and `_validator_execution_error_result` helper.
- `engine.run()` main loop: three-branch classifier
  - INFRA_FAILURE → raise `QualifireInternalError` with `__cause__` chained
  - All-internal → `QualifireInternalError`
  - Real findings (with or without internal alongside) → `QualifireValidationError`
    (data findings take precedence; internal failures still surface in result
    rows for forensic inspection)

### Notifications — `qualifire/notification/base.py`
- `_is_qualifire_internal_failure_row` predicate (sibling to engine's; kept
  separate to avoid circular imports).
- `format_notification_message` and `format_grouped_notification_message`
  filter marked rows so operators don't get false-positive Slack/email pages
  on infra outages.

### Reporting — `qualifire/reporting/{health,html_report}.py`
- `HealthReport.internal_error_count` field + counter logic.
- HTML dashboard exposes `qualifire_internal_failure` boolean to JS.
- `isInternalFailure` JS helper drives `qf-internal-failure` CSS class on
  rendered rows (wrench icon prefix + muted/italic styling).
- Severity pie + summary cards segregate internal-failure rows into a separate
  bucket; pass-rate trend explicitly excludes them.
- `qualifire/storage/sqlite_storage.py` projects `details_json` so the
  predicate can see the marker.

### API + Docs
- `Qualifire.run_config()` and `run_config_parsed()` docstrings document both
  exception classes with routing semantics.
- `docs/notifications.md` "Distinct exception class" section with worked
  except/raise examples and PEP 3134 chaining notes.

## Files Changed
- `qualifire/__init__.py`
- `qualifire/api.py`
- `qualifire/core/engine.py`
- `qualifire/core/exceptions.py`
- `qualifire/notification/base.py`
- `qualifire/reporting/health.py`
- `qualifire/reporting/html_report.py`
- `qualifire/storage/external_catalog.py`
- `qualifire/storage/sqlite_storage.py`
- `tests/test_engine_failure_classification.py` (new — 18 tests)
- `tests/test_storage/test_spark_storages.py` (24 external-catalog tests)
- `docs/notifications.md`
- `docs/features/external-catalog-system-table-hardening/{idea,plan}.md`

## Testing
- 18 new tests in `tests/test_engine_failure_classification.py` covering the
  predicate, validator-execution helper, persistence-infra suppression,
  validator-execution suppression, sibling class contract, mixed-error
  routing, and `PersistenceOutcome` shape.
- 24 external_catalog tests in `tests/test_storage/test_spark_storages.py`
  (13 inverted from old contract + 11 new), including dual-phrase
  Glue/Ranger errors and regex word-boundary substring guards.
- 7 self-review rounds + 8 codex plan-review rounds + 6 codex impl-review
  rounds. Final verdict on both tracks: PASS.

## Notes
- `_persist_results` legacy wrapper still ignores `PersistenceOutcome`; only
  used by test fixtures, deferred as non-blocker.
- Three sibling predicates exist (engine, notification, reporting) by design
  to avoid circular imports — each carries a cross-reference comment noting
  all three must update in lockstep if the marker contract changes.
- Future revisit: Great-Expectations / Deequ-style `validate()` returning a
  result object by default (raise only on infra), captured separately as a
  larger API-shape change.
