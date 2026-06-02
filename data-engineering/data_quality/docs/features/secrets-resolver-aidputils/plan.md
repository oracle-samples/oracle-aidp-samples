---
started: 2026-05-07
---

# Implementation Plan: Secret Resolver — AIDP aidputils Integration

## Overview

Ship a duck-typed `SecretResolver` Protocol and a `secret://name/key`
reference scheme that replaces inline credential values in YAML and
in Python-API arguments. The resolver is supplied by the operator
at `Qualifire.__init__` time (because AIDP's `aidputils` cannot be
imported from custom modules), and references are resolved once at
config-load. The same Protocol generalizes to Vault, AWS Secrets
Manager, and Azure Key Vault backends without further code changes.

### Stated outcome (from `idea.md`)

Operators on AIDP can write:

```python
import aidputils
from qualifire import Qualifire

qf = Qualifire(
    backend=...,
    secret_resolver=aidputils.secrets,
    system_table="catalog.schema.qf_history",
    owner="data-team", bu="finance",
)
```

…then declare credentials by reference rather than value:

```yaml
notifications:
  prod_email:
    type: email
    smtp_host: smtp.company.com
    smtp_port: 587
    smtp_user: "secret://smtp_creds/username"
    smtp_password: "secret://smtp_creds/password"
    sender: "secret://smtp_creds/from_address"
    recipients: ["data@company.com"]

jdbc:
  url: "secret://prod_db/url"
  user: "secret://prod_db/user"
  password: "secret://prod_db/password"
```

…and Qualifire resolves every `secret://...` once **after**
Pydantic model validation but **before** storage / notifier /
engine construction. (Pydantic validators see the literal
`secret://...` strings as ordinary non-empty `str` values; the
two validators that care about credential presence —
`_require_jdbc_when_needed` and `JDBCConfig` mutual exclusion
checks — are updated in P4.2 to recognize `from_secret`. See
Decision Pin 9.)

### What this plan is NOT

- **Not** an `aidputils` integration that imports `aidputils` from
  Qualifire. Qualifire stays AIDP-agnostic; `aidputils.secrets` is a
  duck-typed conformance to the `SecretResolver` Protocol.
- **Not** a vault backend. We ship the Protocol and the AIDP
  conformance contract; concrete vault adapters (Vault, AWS Secrets
  Manager, Azure Key Vault) are downstream — operators write a 5-line
  resolver class and pass it in.
- **Not** a Jinja-engine extension. `secret://...` is resolved by
  a separate post-load walk over validated Pydantic models, not
  by a new Jinja filter or `${VAR}` substitution. (Note:
  `${ENV_VAR}` interpolation does **not** exist in
  Qualifire today — the docs that show it are aspirational and
  are corrected as part of P5 below.)
- **Not** a refactor of the existing notification / JDBC config
  models. The resolution walk happens **on** the existing models,
  not as a new model layer.

## Decision Pins

These resolve the open questions from `idea.md`. The implementation
treats these as inputs, not open questions.

1. **Reference syntax: `secret://name/key`** prefix scheme.
   - Why: existing fields stay `str`-typed; YAML reads naturally;
     Pydantic remains simple. Structured dict form would force
     every credential field into a Union type and bloat the model.
   - **Per-field references must always include both `name` and
     `key`.** The only "whole-secret" path is `from_secret:` on
     `JDBCConfig` (Decision Pin 3), which is a plain secret name,
     not a `secret://...` URL. There is exactly one whole-secret
     directive in the system; `secret://name` (no key) is **not**
     a valid reference and parse-rejects.
   - **Grammar:** the part after `secret://` must contain at
     least one `/`. Split on the **first** `/`. Everything before
     it is `name`; everything after is `key`. `name` therefore
     cannot contain `/`; `key` may contain `/` (some vault paths
     use slashes inside the key). Allowed character class for
     `name` is `[A-Za-z0-9_.-]+` (slash deliberately excluded);
     `key` is `[A-Za-z0-9_./-]+`. Empty `name` or empty `key`
     reject. Other characters reject at parse time with a clear
     error naming the YAML field path.

2. **Resolution timing: eager, exactly once at config-load.**
   - Why: secrets sit in memory for a run's duration anyway (the
     SMTP socket holds the password until the email is sent).
     Eager is dramatically simpler — one walk over the validated
     Pydantic model, no per-use threading of resolver to deep call
     sites. Run lifetimes are seconds-to-minutes; rotation during
     a run isn't a real workflow.
   - Lazy resolution would require mutating notifier / JDBC
     internals; that's a much bigger blast radius.

3. **Whole-secret JSON case: `from_secret:` directive on
   `JDBCConfig`.**
   - `JDBCConfig` gains a `from_secret: str | None = None`
     field. The value is a **plain secret name** (not a
     `secret://...` URL — there is exactly one whole-secret
     directive in the system). Same name grammar as Decision
     Pin 1's `name` segment (`[A-Za-z0-9_.-]+`).
   - When set, the resolver is called as
     `resolver.get(from_secret, key=None)` and is expected to
     return a `dict[str, str]`. Returned keys populate `url`,
     `user`, `password`, `driver` only (see P4.3).
   - **Mutual exclusion with per-field `secret://...` refs**:
     enforced at resolution time (Decision Pin 9). Walker
     scans the JDBC instance: if `from_secret` is set, no other
     field may start with `secret://`. Raises
     `SecretResolutionError` naming the conflicting field.
   - **Mutual exclusion with direct literal values**: if
     `from_secret` is set **and** any of `url`, `user`,
     `password`, `driver` is also set to a non-`None` literal
     value (any non-`secret://` string), the walker raises
     `SecretResolutionError("from_secret precludes inline
     credential values; remove <field>")`. The credential
     source is unambiguous: the secret-store dict, full stop.
     Addresses Codex round-2 finding 10.
   - Without a resolver: `from_secret` set + no resolver raises
     `MissingSecretResolverError`. Addresses Codex round-2
     finding 7.
   - `from_secret` exists on `JDBCConfig` only; notifier
     configs use per-field references. After resolution,
     `from_secret` is cleared on the resolved copy so the
     resolver isn't re-called on a second pass (idempotency).

4. **Missing resolver + references present → raise.**
   - The error message names the first unresolved reference and
     points at `Qualifire(secret_resolver=...)`.
   - No silent passthrough — leaving a literal `secret://...`
     string in a credential field would surface as a confusing
     runtime auth failure inside SMTP / JDBC code.
   - **Resolver present but reference absent** — no-op walk; cost
     is negligible.

5. **Cache by (name, key) within one `resolve_secrets()` call —
   nowhere else.**
   - One `dict[(name, key), Any]` allocated locally inside the
     `resolve_secrets()` invocation, passed down to every
     `_walk_*` dispatch.
   - **Forbidden** to store on `Qualifire`, `QualifireContext`,
     the resolver, or a module-level global. The cache must die
     when the call returns — that's how rotated secrets stay
     fresh on the next call.
   - Across separate `validate*` / `run_config*` calls on the
     same `Qualifire` instance: no cache. The point is to avoid
     N round-trips for the same `(name, key)` *within* one
     resolution pass, not across passes.

6. **Constrained reach: credential fields only, not arbitrary
   Jinja.** `secret://...` resolves only on the explicit allowlist
   below. The walker is **not** a generic recursive `BaseModel`
   walker; it dispatches on model class and walks only the
   listed fields:

   | Model class | Fields walked |
   |---|---|
   | `EmailNotificationConfig` | `smtp_user`, `smtp_password`, `sender` |
   | `SlackNotificationConfig` | `webhook_url` |
   | `WebhookNotificationConfig` | `url`, `headers{}` values |
   | `JDBCConfig` | `url`, `user`, `password`, `driver`, `properties{}` values, `from_secret` |

   **Why `recipients[]` is excluded:** recipients are addressees,
   not credentials, and the email notifier success-path log
   already prints them at INFO level
   (`email_notifier.py:78`). Routing recipients through the
   resolver would either re-leak them through that existing log
   line or force an invasive log refactor. Operators should
   either hardcode recipients in YAML or maintain them outside
   Qualifire's secret store. Documented as a non-goal.

   Walker enters `QualifireConfig.notifications{}` and `JDBCConfig`
   only. It does **not** descend into `DatasetConfig`,
   `CustomQueryCollectionConfig.sql`, `*.filter`,
   `partition_ts`, or any validator/collector field. A
   `secret://...` literal in those fields is preserved verbatim.

   - Allowing it everywhere creates a surprise vector — operators
     expect their SQL to render verbatim.
   - Future extension: extend the allowlist by adding rows above,
     never by removing the dispatch. Out of scope here.

7. **Resolved values do NOT leak to logs.**
   - Resolved credentials never appear in log lines.
   - The resolution walker emits a single info log: "resolved N
     secret references" — no values, no names beyond a count.
   - Existing notifier debug logs that print the URL / smtp_host
     stay (those weren't credentials); fields known to carry
     credentials are masked in any future debug output.

8. **Resolver fault tolerance: re-raise wrapped, no retry, no
   cause chain.**
   - When `resolver.get(name, key)` raises (network blip, secret
     not found, permission denied), the walker raises
     `SecretResolutionError(name, key, field_path,
     original_type=type(e).__name__)` **`from None`**. The
     wrapper message names the reference and the YAML / model
     field path and the *type name only* of the original
     exception; it does **not** include `repr(original)`,
     `str(original)`, or `original.args` (vault SDKs occasionally
     surface the value in error messages — we never echo whatever
     they say). `__cause__` is **not** set, so default tracebacks
     do not print the original exception's message.
   - **No env-var bypass / debug mode.** An earlier draft
     proposed `QUALIFIRE_DEBUG_SECRETS=1` to switch the raise
     to `from e`. Dropped: any shipped bypass is a footgun
     operators set in prod by accident and contradicts gate 7.
     Developers who need the original traceback should attach a
     debugger or temporarily replace the resolver with one that
     surfaces the failure inline — not flip an env var.
   - No retry / backoff. Operators rotate-and-retry at their
     orchestration layer; embedding retry here would mask a real
     vault outage as a config-load slowness.

9. **`from_secret` ↔ per-field `secret://...` mutual exclusion
   is detected at resolution time, not at Pydantic-validate time.**
   - Pydantic validators see `secret://...` strings as ordinary
     non-empty `str` values; they cannot tell a literal from a
     reference. The mutual-exclusion check therefore lives in the
     walker: when `from_secret` is set on a `JDBCConfig`, walking
     the same model after must observe **zero** other fields
     starting with `secret://`. If any are found, raise
     `SecretResolutionError` naming both the `from_secret` name and
     the conflicting field.

## Scope gates (hard-stop conditions)

Implementation MUST stop and escalate to the user if any of the
following emerge:

1. **Scope gate — no `aidputils` import.** If a design pressure
   appears to do `import aidputils` from Qualifire (even as an
   optional / conditional import), stop. The Protocol-injection
   contract is load-bearing for the AIDP-vs-vault decoupling.
2. **Scope gate — no new optional-dependency group.** No `[secrets]`
   extra. The base install already supports the integration; the
   resolver is operator-supplied.
3. **Scope gate — no new collector / validator / notifier types.**
   This feature touches existing models only. If a new
   first-class secret-handling type emerges as necessary, stop.
4. **Scope gate — no Jinja-engine changes.** `secret://...` is
   resolved by a separate post-load walk, not by adding a new
   filter to the Jinja environment. Mixing the two reaches into
   the partition_ts / custom_query rendering paths and changes
   their behavior.
5. **Correctness gate — eager resolution must be idempotent.**
   Calling `resolve_secrets()` twice on the same model produces
   identical results. Once resolved, the field's literal string
   no longer matches `secret://...`, so the second call is a
   no-op walk. Verified by the test:
   - first call: counting-stub resolver call count == N
   - second call (fresh cache): counting-stub resolver call count
     == 0 (not just "result equality" — call count is the gate)
6. **Correctness gate — recipient lists, header maps, properties
   maps walk per-element.** A `secret://...` inside a list or dict
   value resolves; the structure is preserved.
7. **Hard-stop — no value-level masking in error messages.** If
   resolution fails partway, the error message names the
   reference (`secret://smtp_creds/password` not the value) and
   the YAML field path. Never include a partially-resolved value
   or a stack trace from inside the resolver.

## Implementation Phases

Each phase is independently shippable / revertible. Tests land in
the same phase as the code they cover.

### Phase 1 — Protocol + walker (no callers wired yet)

Goal: ship the resolver Protocol, the reference parser, the model
walker, and exhaustive unit tests. Nothing in the live code path
calls `resolve_secrets()` yet.

- [ ] **P1.1** — `qualifire/core/secrets.py` (new) defines:
    - `class SecretResolver(Protocol): get(name, key=None) -> Any`
    - `_SECRET_PREFIX = "secret://"`; `parse_secret_ref(value:
      str) -> tuple[str, str] | None` splits on the first `/`
      after the prefix; returns the `(name, key)` pair, or
      `None` if the string isn't a reference; raises `ValueError`
      for malformed forms (no `/`, empty name, empty key,
      illegal characters per Decision Pin 1).
    - `resolve_secrets(cfg: QualifireConfig | None, resolver:
      SecretResolver | None, *, jdbc: JDBCConfig | None = None)
      -> tuple[QualifireConfig | None, JDBCConfig | None]`:
        - if `cfg` is provided: walks `cfg.notifications` values
          and `cfg.jdbc` (if any) using the allowlist dispatch
        - if `jdbc` is provided as a separate kwarg: walks that
          too (covers the constructor-only JDBC case where
          `cfg` does not exist yet)
        - shares one local `cache: dict[tuple[str, str], Any]`
          across all walks in this call (Decision Pin 5)
        - returns the resolved copies. The originals are never
          mutated. Caller-owned configs passed to
          `Qualifire.run_config_parsed(config)` stay reusable.
    - **Selective copy strategy** (the contract — never
      `copy.deepcopy()`): walker uses
      `model.model_copy(deep=False)` on the four target classes
      and explicitly clones only the **mutated** containers
      (`dict(cfg.headers)`, `dict(cfg.properties)`) before
      writing into them. `QualifireConfig` itself is
      shallow-copied via `model_copy(update={...})` with the
      new `notifications` dict and resolved `jdbc`.
      `DatasetConfig`, `WAPConfig`, and any field carrying a
      live `df: DataFrame` are **never** copied — those
      branches preserve object identity. This addresses Codex
      round-2 finding 2 (Spark/Pandas DataFrames are not
      `deepcopy`-safe; pandas `deepcopy` is also expensive
      enough to skew benchmarks).
    - **Resolved-value type guard (Phase 1 scope):** every
      `_walk_*` checks the resolver's return value against
      the field's expected Python type before assignment:
        - per-field string ref → `str` required
        - dict-value ref (e.g., `headers["Authorization"]`) →
          `str` required
      A type mismatch raises `SecretResolutionError(name, key,
      field_path, expected="str", got=type_name)` — names the
      type, never the value. Addresses Codex round-2 finding 8.
      The `from_secret` `dict[str, str]` type guard lives in
      Phase 4 (P4.3) where the directive is implemented.
    - Internal dispatch is by model class — no generic
      `BaseModel` recursion. Walks only the allowlist fields per
      Decision Pin 6. A `secret://...` literal anywhere outside
      the allowlist is preserved verbatim.
- [ ] **P1.2** — Walker is **allowlist-driven**, not generic.
      Module exposes a single registry
      `_NOTIFICATION_SECRET_WALKERS:
      dict[type[BaseModel], Callable[..., BaseModel]]`
      keyed by exact model type
      (`EmailNotificationConfig`, `SlackNotificationConfig`,
      `WebhookNotificationConfig`) and a separate `_walk_jdbc`
      for `JDBCConfig`. The top-level `resolve_secrets(cfg,
      resolver, jdbc=None)`:
    - **selective copy contract** (the only contract — no
      `copy.deepcopy()`): for each notification entry to mutate
      and for `JDBCConfig`, the walker calls
      `model_copy(deep=False)` on the model and clones only the
      mutated containers (`dict(cfg.headers)`,
      `dict(cfg.properties)`). The parent `QualifireConfig` is
      shallow-copied via `model_copy(update={"notifications":
      new_dict, "jdbc": new_jdbc})`. `DatasetConfig`,
      `WAPConfig`, and any field carrying `df: DataFrame` keep
      object identity — never copied. The caller's original
      `cfg` and the original `notifications` / `headers` /
      `properties` containers remain unchanged.
    - dispatch: iterates `cfg.notifications.values()`, looks up
      `_NOTIFICATION_SECRET_WALKERS[type(entry)]`. **Unknown
      notification type → fail fast** with a registration error
      naming the type — never silently preserve refs.
    - shares one local cache `dict[tuple[str, str], Any]` across
      every dispatch in this call (Decision Pin 5).
    - **String mutation:** each walker reassigns specific
      fields by name (`new.smtp_password = _maybe_resolve(
      new.smtp_password, ctx)`). Dicts of strings
      (`headers`, `properties`) are walked element-wise on
      the cloned dict (`for k, v in d.items(): d[k] = ...`).
      Dict **keys** are never inspected. (`recipients` is
      **not** walked — see Decision Pin 6.)
    - **No generic `BaseModel` recursion**, no `tuple` /
      `frozenset` handling, no `model_dump()` round-trip. The
      four targeted models do not contain credential-bearing
      tuple/frozenset fields in `main`.
    - **Phase-1 scope** covers per-field `secret://...` only.
      `_walk_jdbc` in Phase 1 reads `from_secret` only to raise
      `NotImplementedError("from_secret arrives in Phase 4")`
      if set — no resolver call. Full `from_secret` semantics
      (resolver call, type guard, mutual-exclusion checks)
      live exclusively in Phase 4.
- [ ] **P1.2.b** — `qualifire/core/exceptions.py` adds
      `MissingSecretResolverError` and `SecretResolutionError`,
      both subclasses of the existing `QualifireConfigError`. The
      CLI's existing `Config validation failed:`-prefixed error
      handling already prints `str(e)` for any
      `QualifireConfigError`, so no CLI changes are needed.
- [ ] **P1.3** — Tests in `tests/test_core/test_secrets.py`:
    - reference parser:
        - valid forms: `secret://name/key`,
          `secret://name/path/with/slash` (key may contain `/`)
        - **invalid** forms (parse-reject): `secret://name` (no
          key), empty name, empty key, illegal characters,
          missing prefix, `secret://` alone, `secret:///key`
    - walker exhaustiveness: a registry test asserts every
      member type of `NotificationChannelConfig` is in
      `_NOTIFICATION_SECRET_WALKERS`; a synthetic notifier
      type passed in fails fast with a registration error
    - walker on a flat Pydantic model: per-field resolution
    - walker on nested model: `headers` dict resolves
      element-wise; dict keys are not inspected
    - walker with cache: same (name, key) hit is one resolver
      call across a model
    - walker with no resolver + no references: no-op, no raise
    - walker with no resolver + references: raises with the
      reference path in the message
    - walker idempotency: two calls; second call's resolver
      call count is **0** (gate 5)
    - **caller-config preservation**: the original `cfg` /
      `jdbc` object identity is unchanged; the original
      `headers` / `properties` containers still contain the
      `secret://...` literals after the call
    - **type guard**: resolver returning a non-`str` for a
      string field raises `SecretResolutionError` naming the
      expected/got type, never the value
    - **DataFrame identity**: a `DatasetConfig` carrying a
      sentinel `df` whose `__deepcopy__` raises must survive
      `resolve_secrets()` with the exact same `df` object
      identity (regression test for Codex round-2 finding 2)
    - **__cause__ is None**: on resolver failure, `str(error)`
      and `traceback.format_exception(error)` both omit a
      sentinel value placed in the original exception's
      message (gate 7 + Codex round-2 finding 4)
- [ ] **Exit:** `pytest tests/test_core/test_secrets.py` passes; no
      changes to live config-load or `Qualifire` constructor; the
      module is importable but unused.

### Phase 2 — `Qualifire` constructor wiring + constructor-time JDBC resolution

Goal: thread the operator-supplied resolver into `Qualifire`, and
resolve `jdbc=` constructor-supplied references **before**
`_init_storage()` runs (so JDBC storage opens with real
credentials, not literal `secret://...` strings).

Decision: resolver lives on the `Qualifire` instance only. It is
**not** added to `QualifireContext` — `QualifireContext` is a
Jinja renderer; conflating it with secret resolution made the
data-flow muddy (Codex round-1 finding 8). The walker reads the
resolver from a parameter, not from context.

- [ ] **P2.1** — `qualifire/api.py:Qualifire.__init__` adds a new
      keyword-only `secret_resolver: SecretResolver | None = None`
      parameter (forced keyword via `*,` in the signature; the
      existing positional callers keep working unchanged). Stored
      on `self._secret_resolver`. Docstring includes the AIDP
      one-liner.
- [ ] **P2.2** — Constructor flow change: **only when**
      `system_table is not None` **and** `system_table_backend
      == "jdbc"` and `self.jdbc is not None`, call
      `_, resolved_jdbc = resolve_secrets(cfg=None,
      resolver=self._secret_resolver, jdbc=self.jdbc)` and
      assign `resolved_jdbc` to `self.jdbc` **before**
      `_init_storage()` runs.
    - For non-JDBC backends, constructor `jdbc=` (a templated
      block left over for a future deployment per the existing
      "templated block" tolerance in `_require_jdbc_when_needed`)
      is passed through without resolution; non-credential
      backends never read it.
    - If the JDBC backend is active and `self.jdbc` contains
      `secret://...` references with no resolver, raise
      `MissingSecretResolverError` naming the offending field —
      clear failure at construction time, not at first storage
      write. Addresses Codex round-2 finding 3.
    - **Mirror in `run_config_parsed`:** when the candidate
      backend in `run_config_parsed` is `jdbc` (it can come from
      YAML and override `self.system_table_backend`), the
      candidate `jdbc` block is resolved before
      `_init_storage(...)` is called with it, even if the
      constructor never went through P2.2.
- [ ] **P2.3** — `qualifire/core/context.py` is **not** modified.
      Resolver does not live on `QualifireContext`.
- [ ] **P2.4** — Tests in `tests/test_api.py`:
    - `Qualifire(secret_resolver=mock)` — `qf._secret_resolver is
      mock`
    - `Qualifire()` — `qf._secret_resolver is None`
    - `Qualifire(system_table=..., system_table_backend="jdbc",
      jdbc={"url": "secret://prod_db/url", ...},
      secret_resolver=mock)` → `qf.jdbc.url ==
      mock.values["prod_db", "url"]` (storage opens cleanly)
    - same shape with `secret_resolver=None` →
      `MissingSecretResolverError` at construction
- [ ] **Exit:** `pytest -q tests/test_api.py` passes; existing
      `Qualifire()` callers unaffected.

### Phase 3 — Live resolution at every API entry

Goal: invoke `resolve_secrets()` on the validated model, exactly
once per `validate*` / `run_config*` / `write_audit_publish` call.
This is the phase where resolution becomes live.

Important: resolution is **not** wired into `load_config()`.
`load_config()` is also used by the CLI preflight (`_cmd_run`),
which has no resolver in scope. Programmatic callers also build
`QualifireConfig` instances directly and never pass through
`load_config()`. The clean injection point is **api.py**: every
`Qualifire.{validate,validate_query,run_config,run_config_parsed,
write_audit_publish}` invocation calls `resolve_secrets()` on its
freshly-built / freshly-loaded `QualifireConfig` before handing it
to the engine.

The walker is mutation-free at the caller boundary (Phase 1
contract): `resolve_secrets()` returns selectively-cloned
resolved copies (model_copy(deep=False) + container clones for
mutated fields only — `DataFrame`-bearing branches keep object
identity per the Phase 1 selective-copy contract). The original
caller-owned `QualifireConfig` passed to
`run_config_parsed(config)` stays untouched and remains reusable
across calls — addresses Codex round-1 finding 2.

- [ ] **P3.1** — A new private helper
      `qualifire/api.py:Qualifire._resolve_secrets_or_raise(cfg)
      -> QualifireConfig`:
    - reads `self._secret_resolver`
    - calls `resolved, _ = resolve_secrets(cfg,
      self._secret_resolver, jdbc=None)` — returns the resolved
      copy
    - re-raises any `MissingSecretResolverError` /
      `SecretResolutionError` with dataset name + field path in
      the message
- [ ] **P3.2** — `Qualifire.run_config_parsed()` calls
      `resolved = self._resolve_secrets_or_raise(config)`
      immediately after `load_config(...)` (or after the
      caller-supplied `QualifireConfig` arrives), **before** any
      `_init_storage()` swap, and passes `resolved` to the engine.
      The caller's `config` parameter is never mutated.
- [ ] **P3.3** — `Qualifire.validate()`, `validate_query()`, and
      `write_audit_publish()` call `_resolve_secrets_or_raise(cfg)`
      on the inline `QualifireConfig` they build, before passing
      to the engine. The same call covers their inherited
      `notify=` routes via `QualifireConfig.notifications`. The
      programmatic `validations=` parameter accepts validation
      configs only (no notifier configs) — matches the actual
      shape of `Qualifire.validate(validations=[...])` in
      `qualifire/api.py:285-330`.
- [ ] **P3.4** — Reference cache is a fresh
      `dict[tuple[str, str | None], Any]` allocated per call
      inside `resolve_secrets()`. Not stored on `QualifireContext`;
      not stored on the instance. Scope ends when the call
      returns (rotated secrets cannot get stuck).
- [ ] **P3.5** — Integration tests in `tests/test_api.py`:
    - YAML fixture with `secret://` refs in `notifications:`
      (email + slack + webhook) and top-level `jdbc:` →
      `qf.run_config(...)` resolves them all; assert resolved
      values reach the notifier instances built in the engine
    - Same YAML, `Qualifire()` constructed without
      `secret_resolver` → raises `MissingSecretResolverError` at
      `run_config(...)` time; message names offending reference
      and field path; engine storage state not mutated
    - Programmatic path: `Qualifire(jdbc={"url":
      "secret://prod_db/url", ...}, system_table=...,
      system_table_backend="jdbc", secret_resolver=mock)` →
      storage opens with resolved url (covers the constructor-time
      JDBC path from P2.2 end-to-end)
    - Reuse path: same `QualifireConfig` passed twice into
      `run_config_parsed(config)` resolves correctly both times,
      and the caller's `config` object's `secret://...` literals
      are still present after the call (proving no caller-side
      mutation)
- [ ] **Exit:** `pytest -q` passes on the full sweep; the four
      new integration cases pass.

### Phase 4 — `from_secret:` directive on `JDBCConfig`

Goal: support the whole-secret-JSON case for the most common
container — JDBC.

- [ ] **P4.1** — `qualifire/core/config.py:JDBCConfig` adds
      `from_secret: str | None = None`. **Mutual exclusion is
      enforced at resolution time, not at Pydantic-validate time**
      (see Decision Pin 9): the walker checks "if `from_secret`
      is set, no other field starts with `secret://`."
      Pydantic-time validation cannot tell a literal from a
      `secret://...` reference and would over-reject.
- [ ] **P4.2** — `qualifire/core/config.py:QualifireConfig.
      _require_jdbc_when_needed` is updated to accept either
      `jdbc.url` **or** `jdbc.from_secret` as satisfying the
      "jdbc backend needs creds" gate. Without this change,
      `system_table_backend="jdbc"` + `jdbc: {from_secret:
      "prod_db"}` would fail Pydantic validation before the
      walker ever runs (Codex round-1 finding 4).
- [ ] **P4.3** — `resolve_secrets()` walker special-cases the
      `from_secret` field: when present, fetches the whole secret
      via `resolver.get(name)`, expects a dict result, and merges
      keys into the corresponding direct fields of `JDBCConfig`
      (`url`, `user`, `password`, `driver` — exactly these four).
      `properties` is **not** populated from the secret —
      operators set `properties` directly in YAML. Unknown keys
      in the secret (anything outside the four direct fields)
      raise with both the unexpected key and the allowed-key
      list in the message. Missing `url` (the only field
      `_require_jdbc_when_needed` requires post-resolution)
      raises with `url` in the message. After resolution,
      `from_secret` is cleared on the resolved copy so a second
      walk is a no-op.
- [ ] **P4.4** — Tests:
    - `from_secret` populates fields when secret returns a dict
    - mutual exclusion of `from_secret` + per-field refs raises
      `SecretResolutionError` (the walker check, not Pydantic)
    - `system_table_backend="jdbc"` + `jdbc: {from_secret:
      "prod_db"}` constructs cleanly, walker resolves, storage
      opens (full P2.2 + P4.3 path)
    - secret dict missing required key (`url`) raises with the
      missing-key name in the message
    - secret dict with extra keys raises with the unexpected-key
      name (don't silently drop — typo'd key would otherwise
      lose data)
- [ ] **Exit:** `pytest -q tests/test_config.py tests/test_api.py
      tests/test_core/test_secrets.py` passes; doc snippet in the
      JDBC section of `docs/configuration.md` (or
      `docs/notifications.md` if that's where JDBC actually lives)
      shows the directive.

### Phase 5 — Notifier log / persisted-message sanitization

Goal: keep gate 7 ("resolved credentials never appear in log
lines, never in persisted system-table rows") honest. Without this
phase, gate 7 is a sticker, not an enforcement (Codex round-1
finding 5).

Concretely, on `main`:

- `WebhookNotifier.send()` logs `self.url` at INFO
  (`webhook_notifier.py:84`). After resolution, that URL may carry
  a secret token (e.g., Slack incoming-webhook URLs are bearer
  secrets in the path).
- `SlackNotifier.send()` posts to `self.webhook_url` and on failure
  returns `NotificationResult(message=str(e))`
  (`slack_notifier.py:55-60`). `requests`'s exception messages
  include the request URL, so the secret webhook URL ends up in
  the message.
- `WebhookNotifier.send()` failure path: same — `str(e)` may
  embed the URL.
- `EmailNotifier.send()` failure path: same — `str(e)` from
  `smtplib` includes the SMTP host / port (not credentials by
  themselves, but co-locating host + a credential-shaped error is
  the classic auth-failure leak vector).
- `engine.py` persists `NotificationResult.message` into the
  system table (`engine.py:1547-1549`), so the leak is **durable**,
  not just in-memory log lines.

Scope of this phase (kept narrow — do not reach beyond):

- [ ] **P5.1** — Add `qualifire/notification/_redact.py` with a
      single helper `redact_url(url: str) -> str` that returns
      `"{scheme}://{host}/<redacted-path>"`. Tested for: Slack
      webhook URLs (path is the secret), generic webhooks with
      bearer-in-query, and malformed inputs (returns
      `"<unparseable-url>"` rather than raising).
- [ ] **P5.2** — `WebhookNotifier.send()`:
    - INFO success log uses `redact_url(self.url)` instead of
      `self.url`
    - failure path: `NotificationResult.message` and `logger.error`
      both use `redact_url` over the raw exception message;
      preserve the exception class name for diagnostic value
      (`f"{type(e).__name__}: {redact_url(self.url)}"`)
- [ ] **P5.3** — `SlackNotifier.send()`: same pattern as P5.2.
- [ ] **P5.4** — `EmailNotifier.send()`:
    - failure path: `NotificationResult.message` is
      `f"{type(e).__name__}: SMTP send failed"` (host is already
      in the success log; no need to repeat in error). Original
      exception attached as `__cause__` for stack traces in
      development.
- [ ] **P5.5** — Tests in `tests/test_notification/` (new dir if
      needed):
    - log-capture fixture asserts `secret-token-value` does **not**
      appear in any captured INFO/DEBUG/ERROR record after a
      simulated send (success and failure paths, all three
      notifier types)
    - `NotificationResult.message` does not contain the resolved
      secret in the failure path
- [ ] **Exit:** notifier tests pass; running the full integration
      test from P3.5 with a stub resolver returning "literally a
      secret value" and `caplog` set to DEBUG produces zero log
      records containing the secret value.

### Phase 6 — Documentation

- [ ] **P6.1** — `docs/jinja_rendering.md` gains a "Secrets" section
      explaining the prefix scheme, the AIDP injection one-liner,
      and the constrained reach (credential fields only, not
      arbitrary Jinja).
- [ ] **P6.2** — `docs/notifications/email.md`,
      `docs/notifications/slack.md`,
      `docs/notifications/webhook.md` **delete** the misleading
      `${ENV_VAR}` examples and the "accepts `${ENV_VAR}`
      interpolation" notes (no such interpolation exists today —
      see Problem Statement in `idea.md`), and replace with
      `secret://...` examples plus a "or pass via the Python API"
      paragraph for non-AIDP operators. Same fix in the inline
      docstring of `JDBCConfig` in `qualifire/core/config.py`.
- [ ] **P6.3** — `docs/getting_started.md` AIDP section gains a
      paragraph on `secret_resolver=aidputils.secrets`.
- [ ] **P6.4** — README install snippet adds the AIDP one-liner.
- [ ] **P6.5** — `docs/jinja_rendering.md` documents the
      out-of-scope cases (Jinja-rendered fields, programmatically
      registered notifier instances) so operators aren't surprised
      when `secret://...` in `custom_query.sql` stays literal.
- [ ] **Exit:** doc grep `secret://` returns hits in all the
      pages above; no doc still claims "credentials must be set
      via env vars" without also mentioning the resolver.

## Testing Strategy

| Layer | What's tested | File |
|---|---|---|
| Unit | reference parser, walker registry exhaustiveness, type guards, cache, idempotency, caller-config preservation, DataFrame identity, `__cause__ is None` | `tests/test_core/test_secrets.py` (new) |
| Unit | `Qualifire` constructor + constructor-time JDBC resolution | `tests/test_api.py` |
| Unit | `JDBCConfig.from_secret` (mutual exclusion at resolution time, dict-shape errors, overlap with literals) | `tests/test_config.py` + `tests/test_core/test_secrets.py` |
| Unit | notifier log/persisted-message redaction (`tests/test_notification/test_redact.py`, `test_log_capture.py`) | (new) |
| Integration | end-to-end `qf.run_config(...)` + constructor-jdbc with a stub resolver — references in YAML resolve and reach notifier instances | `tests/test_api.py` |
| Integration | missing-resolver error path (references present, resolver `None`) at construction time and at `run_config` time | `tests/test_api.py` |
| Integration | reuse path: same `QualifireConfig` passed twice into `run_config_parsed` resolves both times; caller's config still has literals | `tests/test_api.py` |
| Doc | `docs/jinja_rendering.md` + per-notifier docs reference the new shape | manual review |
| Manual / smoke | AIDP notebook smoke check (separate from CI) — see Acceptance criterion 2.b | `docs/features/secrets-resolver-aidputils/aidp_smoke.md` |

The stub resolver for tests is a `dict[(name, key), value]` map
with a `.get(name, key=None)` method — five lines.

## Acceptance / Exit criteria

The feature is shippable when **all** of these hold:

1. ✅ `pytest -q` passes; new tests cover every Decision Pin.
2.a ✅ **CI-verifiable duck-typed resolver test**: a five-line
   stub resolver class with a single `get(name, key=None)`
   method (no Qualifire inheritance) works end-to-end against a
   fixture YAML carrying `secret://...` references in email +
   slack + webhook + JDBC fields, plus an inline `from_secret:`
   on `JDBCConfig`. The test runs in CI; passes without `aidputils`
   installed.
2.b ✅ **Manual AIDP smoke checklist** in
   `docs/features/secrets-resolver-aidputils/aidp_smoke.md`
   spelling out: owner, target AIDP environment, fixture YAML
   path, the exact one-liner `Qualifire(secret_resolver=
   aidputils.secrets, ...)`, and the expected evidence
   (notification delivers, no `secret://` literals in logs).
   This runs once before tagging the feature shipped, not in CI.
   **CI substitute** in
   `tests/test_api_secrets.py::TestAidpSmokeShape` covers every
   end-to-end shape except the genuinely AIDP-only bits (real
   `aidputils`, real Slack delivery, AIDP-side audit). See the
   "CI substitute" section in `aidp_smoke.md` for the mapping.
3. ✅ Calling `Qualifire()` without `secret_resolver` and a YAML
   that contains `secret://...` references raises
   `MissingSecretResolverError` (a `QualifireConfigError`
   subclass) with the offending reference and field path in the
   message.
4. ✅ A run with no `secret://...` references and no resolver is a
   no-op: the walker visits the model once and exits without
   calling `resolver.get(...)` even once. Verified by
   `tests/test_core/test_secrets.py::test_walker_no_refs_no_resolver_calls`
   using a counting stub resolver — call count must be 0.
5. ✅ `docs/jinja_rendering.md` documents the prefix scheme, reach
   constraint, AIDP injection, and the out-of-scope cases.
6. ✅ **Resolved credential values do NOT appear in any of:**
   `INFO` log records, `DEBUG` log records, `ERROR` log records,
   `NotificationResult.message` (in-memory), or
   `qf_notifications` system-table rows persisted by the engine.
   Verified by `caplog`-driven tests in `tests/test_notification/`
   that exercise success and failure paths for all three notifier
   types with a sentinel resolved secret.
7. ✅ The resolver Protocol is duck-typed — a 5-line custom
   resolver class works without inheriting anything from
   Qualifire (already covered by criterion 2.a).

## Out of Scope (explicit non-goals)

- A bundled vault adapter (Vault / AWS Secrets Manager / Azure Key
  Vault). The Protocol is the integration contract; concrete
  adapters live in operator code.
- Secret rotation mid-run (lazy resolution path). Decided eager.
- Allowing `secret://...` inside custom query SQL or arbitrary
  Jinja templates. Constrained to credential fields per Decision
  Pin 6.
- Encrypted-at-rest config files. That's a different feature.
- A Qualifire-side audit log of "which secrets were resolved
  during this run." Operators on AIDP get this from the vault
  side; doubling here is redundant.
- **Programmatically-registered `Notifier` instances**:
  `qf.register_notifier("prod", EmailNotifier(...))` passes
  fully-constructed notifier objects whose credential fields are
  plain Python attributes, not Pydantic models. The walker does
  **not** see them. Operators using the programmatic registration
  path are responsible for calling their own resolver before
  building the notifier. Documented in `docs/jinja_rendering.md`.
- **Walker traversal of dict keys**: only dict *values* are
  walked. A `secret://...` literal placed as a key in a
  `headers` map remains a literal key. (Real-world keys are
  fixed strings like `Authorization`; never a secret.)

## Risks

- **AIDP API drift.** If `aidputils.secrets.get` signature changes
  upstream (e.g., adds a required `version=` arg), the operator's
  resolver injection breaks. Mitigation: documented Protocol
  contract is `get(name, key=None) -> Any`; operators wrap
  non-conforming backends in a 5-line adapter.
- **Cache persistence beyond run lifetime.** The (name, key)
  cache must not survive across `validate*` / `run_config*`
  calls — otherwise rotated secrets stay stale within the same
  Qualifire instance. Mitigation: cache is a local dict inside
  `resolve_secrets()` (Decision Pin 5); never attached to
  `Qualifire`, `QualifireContext`, the resolver, or a
  module-level global. Test in `tests/test_core/test_secrets.py`
  proves a second `resolve_secrets(...)` invocation re-issues
  the resolver call (count > 0).
- **`from_secret` dict shape mismatch.** AIDP teams may store JDBC
  creds with non-standard key names (`username` vs `user`, `dsn`
  vs `url`). Mitigation: P4.3 raises with both the expected and
  observed key names so the operator can either rename in the
  vault or write a small wrapping resolver.

## Effort estimate

Small-Medium. ~6–10 hours of code + tests + docs, broken across
six phases (Phases 1–4 ship the runtime; Phase 5 sanitizes
notifier logs/persisted messages; Phase 6 is documentation). Most
surface area is in Phase 5 (notifier sanitization) and Phase 6
(docs); the core runtime change is ~200 lines plus ~300 lines of
tests.
