---
id: secrets-resolver-aidputils
name: Secret Resolver — AIDP aidputils Integration
type: Feature
priority: P1
effort: Small
impact: High
created: 2026-05-07
---

# Secret Resolver — AIDP aidputils Integration

## Problem Statement

Qualifire configurations carry credentials inline today: SMTP
passwords, Slack webhook URLs, webhook bearer tokens, JDBC user /
password pairs. There is currently **no in-process secret
substitution mechanism**. The existing
`docs/notifications/email.md` example shows `smtp_password:
"${SMTP_PASSWORD}"` and the code-comment in
`qualifire.core.config.JDBCConfig` says "secrets should be injected
via environment variables at config-load time (Jinja substitution,
templating tool, or direct programmatic construction)" — but
`load_config()` does not perform any `${ENV_VAR}` expansion. An
operator using that snippet today would send the literal string
`"${SMTP_PASSWORD}"` as the SMTP password.

The two paths that actually work today are:

1. The Python API: build the `EmailNotifier` /
   `WebhookNotifier` / `JDBCConfig` instance directly and pass
   resolved credentials as kwargs.
2. A pre-load templating step that the operator owns (e.g.,
   `envsubst < config.yaml` before `load_config()`).

Neither works on AIDP. AIDP provides a managed
`aidputils.secrets.get(name, key=None)` API that returns secret
values from a vault. Operators on AIDP currently have two choices:
drop credentials into YAML (audit-flag), or wrap every
config-load call in their own pre-render that swaps secret
references for resolved values.

A second wrinkle is specific to AIDP's runtime model: `aidputils` is
auto-imported into notebook globals but is **not** importable from
custom Python modules the notebook imports. Qualifire is one such
custom module — it cannot do `import aidputils` itself. The secret
resolver has to be passed into Qualifire from the calling notebook.

## Why It Matters

- Compliance teams reject configs that ship credentials in plaintext
  YAML, even at rest in source control. AIDP teams currently
  side-step this with notebook-level pre-rendering — fragile, easy to
  get wrong.
- The same shape (resolver-as-injected-callable) extends naturally
  to other secret backends (Vault, AWS Secrets Manager, Azure Key
  Vault) without coupling Qualifire to any of them. The duck-typed
  Protocol becomes the integration contract.
- Removes the "every operator hand-rolls their own secret pre-render
  step" tax for first-day AIDP onboarding.

## Who Benefits

- Operators running Qualifire on AIDP Workbench (the primary
  delivery target).
- Security / compliance reviewers who currently flag the inline
  credential pattern.
- Any future operator on a managed-secret-backend platform — the
  same protocol works for Vault / AWS Secrets Manager / Azure Key
  Vault adapters.

## Affected Areas

- `qualifire/api.py` (Qualifire constructor — new
  `secret_resolver` parameter; resolution invocation at every
  `validate*` / `run_config*` / `write_audit_publish` entry)
- `qualifire/core/context.py` (carries the resolver alongside
  other per-call state — every QualifireContext build site in
  `api.py` threads the same resolver)
- `qualifire/core/config.py` (post-load secret resolution walk
  invoked from `api.py`, not from `load_config()` itself, so
  programmatic callers that bypass `load_config()` still get the
  walk)
- `qualifire/core/secrets.py` *new* (Protocol + walker + reference
  parser + AIDP integration test stub)
- `qualifire/notification/{email,slack,webhook}_notifier.py`
  (consumers of resolved values — no notifier-side changes if the
  resolution is fully eager)
- `docs/jinja_rendering.md` (secrets section)
- `docs/notifications/{email,slack,webhook}.md` (the misleading
  `${ENV_VAR}` examples replaced with `secret://...` form **and**
  the honest "use the Python API" alternative — the `${VAR}` form
  is removed because it never worked)
- `docs/getting_started.md` (AIDP secret-resolver section)

## Open Questions for Planning

(For `plan.md` to resolve — not committing here.)

- **Reference syntax**: prefix-scheme `secret://name/key` vs.
  structured dict form `{secret_name, secret_key}`. Recommendation
  is the prefix scheme so existing string-typed Pydantic fields
  stay simple, but plan should explicitly evaluate.
- **Resolution timing**: eager (resolve once at config-load) vs.
  lazy (resolve per-use). Eager is simpler; lazy gives tighter
  rotation. Pick one.
- **Whole-secret JSON case**: `aidputils.secrets.get(name)` (no
  key) returns a dict. How does that map onto Pydantic models that
  expect a flat structure? `{from_secret: "name"}` directive, or
  per-field references that all share the same name with different
  keys?
- **Behavior when references exist but no resolver is supplied**:
  raise (forces explicit opt-in), warn (eats failures), or
  passthrough (leaves the literal `secret://...` string in the
  config and let the consumer error)?
- **Caching across runs in the same process**: do we re-resolve a
  reference every `validate()` call, or memoize by (name, key)?
  AIDP's secret backend is fast but consumes API quota.
- **Reference syntax inside Jinja templates**: should `secret://...`
  work inside arbitrary Jinja-rendered fields (custom_query SQL,
  filter expressions), or only on dedicated credential fields?
  Constraining to credential fields is safer; allowing everywhere
  is more flexible.
