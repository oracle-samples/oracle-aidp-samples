---
shipped: 2026-05-08
---

# Shipped: Secret Resolver — AIDP aidputils Integration

## Summary

Operators on AIDP (and any vault-style platform) can now declare
credentials in YAML or programmatic configs as
`secret://name/key` references and inject a duck-typed resolver at
`Qualifire(secret_resolver=...)`. The resolver is the integration
contract — `aidputils.secrets`, Vault, AWS Secrets Manager, and
Azure Key Vault all work without Qualifire ever importing them.

The whole-secret form `from_secret: <name>` on `JDBCConfig` covers
the case where a single vault entry contains the full
`{url, user, password, driver}` bundle.

Resolved credentials never reach logs or persisted system-table
rows: notifier success / failure paths and JDBCStorage
read / write / initialize paths route URLs through `redact_url` /
`redact_jdbc_url` and re-raise Spark/JDBC exceptions through a
URL-scrubbing wrapper before they reach the engine's persistence
layer.

## Key Changes

- New `qualifire/core/secrets.py` with the `SecretResolver`
  Protocol, `parse_secret_ref`, and an **allowlist-driven**
  `resolve_secrets()` walker (per exact model class, not generic
  `BaseModel` recursion). `secret://...` literals outside the
  allowlist (e.g., inside `custom_query.sql`, `filter`,
  `partition_ts`) stay literal.
- New `MissingSecretResolverError` and `SecretResolutionError`
  exceptions, both `QualifireConfigError` subclasses; resolver
  failures re-raise `from None` with the original-exception
  type-name only (vault SDKs occasionally echo values; we never
  do).
- `Qualifire.__init__(*, secret_resolver=None)`. Constructor-time
  JDBC resolution scoped to `system_table_backend == "jdbc"` with
  cheap pre-check (`_has_secret_refs_or_directive`) so non-secret
  configs skip the walker overhead. JDBC resolution mirrored in
  `_init_storage` for the late-`system_table` path.
- Resolution wired into `run_config_parsed`, `validate`,
  `validate_query`, and `write_audit_publish` before any storage
  swap or engine construction. Caller's `QualifireConfig` is
  never mutated (selective `model_copy(deep=False)` + container
  clones for mutated fields only; `DataFrame`-bearing branches
  preserve object identity).
- `JDBCConfig.from_secret` directive added; `_require_jdbc_when_needed`
  validator accepts it as a substitute for `url`.
- New `qualifire/notification/_redact.py` with `redact_url` (HTTP,
  rebuilt from `hostname` + `port` to drop basic-auth userinfo)
  and `redact_jdbc_url` (preserves sub-protocol / host / port / db,
  scrubs `//user:pwd@`, `?password=`, `;password=`, brace- and
  quote-quoted values).
- `WebhookNotifier` / `SlackNotifier` / `EmailNotifier` failure
  paths use `redact_url` and exception type-name only — no `str(e)`
  in `NotificationResult.message` or in `logger.error` calls.
- `JDBCStorage`: precomputed `self._safe_url`; every public read /
  write method decorated with `_scrubbed` so Spark/JDBC exceptions
  can't carry the connection URL into engine logs or persisted
  system-table rows.
- Class-level docstring on `Qualifire` documents thread-safety:
  not safe for concurrent calls; per-call resolver cache is safe
  but instance-level mutations (`system_table`, `jdbc`,
  `_storage`, `_notifiers`) are not. Operators run one Qualifire
  per worker.

## Files Changed

### New

- `qualifire/core/secrets.py`
- `qualifire/notification/_redact.py`
- `tests/test_secrets.py`
- `tests/test_api_secrets.py`
- `tests/test_notification/test_redact.py`
- `docs/features/secrets-resolver-aidputils/aidp_smoke.md`

### Modified

- `qualifire/api.py` — constructor `secret_resolver=` kwarg,
  resolution wired at every API entry, thread-safety doc.
- `qualifire/core/exceptions.py` — `MissingSecretResolverError`,
  `SecretResolutionError`.
- `qualifire/core/config.py` — `JDBCConfig.from_secret`,
  `_require_jdbc_when_needed` accepts directive.
- `qualifire/notification/{email,slack,webhook}_notifier.py` —
  failure-path redaction.
- `qualifire/storage/jdbc_storage.py` — `_safe_url`, `_scrubbed`
  decorator, `_scrub` exception wrapper for `write_results`.
- `docs/jinja_rendering.md` — new "Secrets" section.
- `docs/getting_started.md` — AIDP one-liner.
- `docs/configuration.md` — JDBC `from_secret` reference.
- `docs/notifications/{email,slack,webhook,grouping}.md` —
  replaced misleading `${ENV_VAR}` examples with `secret://...`.

## Testing

- **914 pytest** sweep passes (was 882 before the feature).
- 39 secrets unit tests (`tests/test_secrets.py`): parser,
  registry exhaustiveness, walker per-class, type guards, cache,
  idempotency, caller-config preservation, `DataFrame` identity
  under selective copy, no-leak on resolver failure
  (`__cause__ is None`, value never in traceback),
  `from_secret` directive (mutual-exclusion variants, missing
  required keys, unexpected keys, non-dict resolver value).
- 14 API integration tests (`tests/test_api_secrets.py`):
  constructor JDBC resolution, `run_config_parsed` end-to-end,
  caller-config reuse path, missing-resolver failure modes,
  templated-jdbc-on-non-jdbc-backend tolerance preserved,
  late-`_init_storage` JDBC resolution, malformed-ref → clean
  error, plus `TestAidpSmokeShape` (5 tests acting as CI
  substitute for the manual AIDP smoke — see Notes below).
- 33 redaction tests (`tests/test_notification/test_redact.py`):
  HTTP and JDBC URL redaction including bypass attempts
  (multi-`@`, slash-in-password, brace-quoted SQL Server values,
  query-string `email=...@...`, basic-auth HTTP).

## Review iterations

Plan: 3 self-driven adversarial rounds + 3
`codex-reviewer:feature-review-plan` rounds. Major plan
corrections: walker is allowlist-driven (not generic recursion);
selective copy strategy; constructor JDBC resolution scoped to
JDBC backend; `recipients[]` excluded from allowlist; Phase 5
notifier sanitization added; `__cause__` chain dropped; cache
strictly per-call; grammar split-on-first-slash; `from_secret`
mutual-exclusion at resolution time; resolved-value type guards.

Implementation: 3 self-driven adversarial rounds + 4
`codex-reviewer:feature-review-impl` rounds. Major impl
corrections: `model_fields_set` preservation across resolution
(otherwise downstream `jdbc_explicit` / `backend_explicit` checks
were corrupted); JDBCStorage URL leakage closed across every
read / write boundary; `redact_jdbc_url` robust against multi-`@`,
slash-in-password, brace / quote-quoted values, mixed `;` / `&`
delimiters; `redact_url` rebuilt from `hostname` + `port`;
constructor JDBC resolution centralized in `_init_storage`;
manual smoke procedure reshaped to construct
`Qualifire(backend=..., secret_resolver=...)` only (no
constructor-time `system_table` / `jdbc`) so storage opens via
`run_config_parsed`; CI substitute aligned with that shape.

## Notes / Follow-ups

### **Outstanding acceptance gate: manual AIDP smoke**

Plan acceptance criterion 2.b calls for a one-time manual smoke
against a real AIDP workbench before this record was written.
That smoke **has not run yet** at the time of shipping — the code
is feature-complete, fully reviewed, and CI-validated, but the
genuinely AIDP-only confirmations (real `aidputils.secrets`
duck-conformance, live Slack delivery, AIDP-side audit) are
pending.

**Action required before claiming production-readiness on AIDP:**
follow `docs/features/secrets-resolver-aidputils/aidp_smoke.md`
end-to-end on an AIDP workbench with the three pre-provisioned
test secrets, capture the cell output, and append the smoke result
to this file under a new "AIDP smoke result" section. CI substitute
(`TestAidpSmokeShape`) covers every shape the smoke checks
*except* those AIDP-specific items.

### Pre-existing concurrency hazard documented, not fixed

`Qualifire` is not thread-safe — `run_config_parsed`,
`validate`, etc. mutate `self.jdbc` / `self._storage` /
`self._notifiers` without locking. This race pre-existed on
`main`; this feature makes it slightly more consequential
(wrong-credentials race vs. wrong-storage-handle race). Fix is
out of scope: documented in the `Qualifire` class docstring,
mitigated operationally by the "one Qualifire per worker"
recommendation. If a real concurrent-validation use case
emerges, lift it to its own feature.

### Out-of-scope for this delivery

- Bundled Vault / AWS Secrets Manager / Azure Key Vault adapters
  (the duck-typed Protocol is the integration contract).
- Lazy / mid-run secret rotation (eager-once-per-call by design).
- `secret://...` references inside arbitrary Jinja-rendered
  fields (`custom_query.sql`, `filter`, etc.) — explicit
  allowlist by model class only.
- Programmatically-registered notifier instances
  (`qf.register_notifier("prod", EmailNotifier(...))`) — the walker
  doesn't see fully-built notifier objects; operators must resolve
  before constructing.
- Plain `${ENV_VAR}` interpolation (never existed in Qualifire;
  doc examples that suggested it have been removed).
