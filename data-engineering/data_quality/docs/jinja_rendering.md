# Jinja Rendering — `context` vs `extra`

Almost every string field Qualifire reads is Jinja-rendered before
use: collector SQL, `slice_value`, `partition_ts`, dataset filters,
custom query bodies. This page documents the variable precedence and
which mechanism to use when.

## Built-in variables

Always available, no extra setup:

| Variable | Value | Notes |
|----------|-------|-------|
| `ds` | run logical date as `YYYY-MM-DD` | mirrors Airflow's `ds` |
| `ds_nodash` | run logical date as `YYYYMMDD` | mirrors Airflow's `ds_nodash` |
| `today` | alias for `ds` | for code that reads more naturally |
| `data_interval_start` | run logical timestamp ISO format | sub-day cadences |
| `partition_ts` | resolved partition_ts ISO format | when set on the dataset |
| `run_id` | the engine's run identifier | for tagging |
| `table` | the current dataset's table name | inside collector SQL |

## Two ways to inject your own variables

### `context=` — instance-wide, persists across `validate()` calls

```python
from qualifire import Qualifire
from qualifire.core.context import QualifireContext

qf = Qualifire(
    backend=...,
    system_table=...,
)

# Context attached to the engine; every validate() call in this
# session sees these vars.
qf.context = QualifireContext(
    extra_context={
        "environment": "prod",
        "region": "us-east-1",
    }
)

result = qf.validate(table="catalog.schema.sales", validations=[...])
```

When to use: **session-wide constants** that don't change between
runs. Environment, region, deployment cohort.

### `extra=` (per-render) and `context=` (per-validate())

```python
result = qf.validate(
    table="catalog.schema.sales",
    validations=[...],
    context={"ds": "2026-04-28", "tenant": "alpha"},  # per-call
)
```

The `context=` kwarg on `validate()` / `validate_query()` /
`run_config()` adds to `extra_context` for the duration of that call,
on top of what the instance already carries.

When to use: **per-run values** like the logical date, the tenant
being processed, a backfill target.

## Precedence

The renderer's own merge order in `QualifireContext.render()`:

```
built-in vars  →  instance extra_context  →  per-call extra=
```

Last wins. So:

```python
qf.context = QualifireContext(extra_context={"ds": "2026-01-01"})
# call-time `context=` overrides:
qf.validate(..., context={"ds": "2026-04-28"})
# template "{{ ds }}" → "2026-04-28"
```

This means you can pin a default at instance level and override it
per call without rebuilding the engine.

## YAML-level context

```yaml
context:
  environment: "prod"
  region: "us-east-1"
```

Top-level `context:` in a YAML config feeds `extra_context` when
loaded via `qf.run_config(...)`. Same precedence — call-time
`context=` overrides.

## Where Jinja is rendered

Field-by-field:

| Field | Rendered? | Notes |
|-------|-----------|-------|
| `DatasetConfig.filter` | yes | applied to every collector for the dataset |
| `DatasetConfig.partition_ts` | yes | per-row literal or column ref |
| `AggregationCollectionConfig.expressions[]` | yes | `{{ table }}` works in expressions |
| `AggregationCollectionConfig.filter` | yes | AND-combined with dataset filter, post-render |
| `MetricsCollectionConfig.metrics{}` (values) | yes | each expression rendered |
| `ProfilingCollectionConfig.filter` | yes | |
| `CustomQueryCollectionConfig.sql` | yes | full body — most flexibility lives here |
| `SampleCollectionConfig.slice_value` | yes | parsed as ISO date / datetime after render |
| `SampleHistoryConfig.filters[]` | yes | each verbatim filter rendered |
| `RecencyCollectionConfig.sql` | yes | for `custom_sql` strategy |

Validator-level fields (`thresholds`, `compare`, `model.step`) are
**not** Jinja-rendered — they're parsed as YAML literals, and stuffing
template syntax there raises a clear config-load error.

## Common pitfalls

- **Quoting** — the rendered value is substituted as raw text. So
  `WHERE ds = {{ ds }}` produces `WHERE ds = 2026-04-28` (no quotes,
  invalid SQL). Always wrap: `WHERE ds = '{{ ds }}'`.
- **Variable shadowing** — passing `context={"table": "foo"}` shadows
  the engine-injected `{{ table }}` (the dataset's actual table).
  Don't reuse built-in names unless you mean to override.
- **`partition_ts` rendering** — for sample collections,
  `slice_value` is rendered, then *parsed as ISO* — so
  `slice_value="{{ ds_nodash }}"` (e.g. `"20260428"`) doesn't parse
  and the sampler refuses. Use `ds` (with dashes), not `ds_nodash`.
- **Stale instance context** — if you mutate `qf.context.extra_context`
  in place across runs, you'll get cache hits where you didn't expect
  them (the engine memoizes the rendered values per-render-call but
  not across calls). Build a fresh `QualifireContext` per logical run.

## Secrets

Credential fields (SMTP password, Slack webhook URL, JDBC user /
password, generic webhook auth headers) accept a separate
`secret://name/key` reference scheme that is resolved by an
operator-supplied resolver, **not** by the Jinja engine. The walk
that resolves these is independent of all Jinja rendering — it
runs on the validated Pydantic config after `model_validate(...)`,
before storage / notifier / engine construction.

### One-line setup on AIDP

```python
import aidputils
from qualifire import Qualifire

qf = Qualifire(
    backend=...,
    secret_resolver=aidputils.secrets,  # duck-typed Protocol
    system_table="catalog.schema.qf_history",
    owner="data-team", bu="finance",
)
```

Then declare credentials by reference inside YAML:

```yaml
notifications:
  oncall_email:
    type: email
    smtp_host: "smtp.company.com"
    smtp_password: "secret://smtp_creds/password"
    sender: "qualifire@company.com"
    recipients: ["data@company.com"]
  oncall_slack:
    type: slack
    webhook_url: "secret://slack_creds/oncall_webhook"

jdbc:
  url: "secret://prod_db/url"
  user: "secret://prod_db/user"
  password: "secret://prod_db/password"
```

Or use the whole-secret form on `JDBCConfig`:

```yaml
jdbc:
  from_secret: "prod_db"   # resolver.get("prod_db") returns
                            # {url, user, password, driver}
```

`from_secret` and per-field `secret://` references on the same
`JDBCConfig` are mutually exclusive — pick one source.

### Reference grammar

- `secret://name/key` — split on the **first** `/` after the prefix.
  `name` is everything before the slash; `key` is everything after.
- `name` allowed characters: `[A-Za-z0-9_.-]+` (no slashes).
- `key` allowed characters: `[A-Za-z0-9_./-]+` (slashes allowed —
  some vault paths embed slashes in keys).
- `secret://name` (no key) is **not** valid; use `from_secret:` on
  `JDBCConfig` for the whole-secret form.

### Where references resolve

`secret://...` references resolve only inside an explicit
**credential allowlist**. They are preserved verbatim everywhere
else (including all Jinja-rendered fields):

| Model class | Fields walked |
|---|---|
| `EmailNotificationConfig` | `smtp_user`, `smtp_password`, `sender` |
| `SlackNotificationConfig` | `webhook_url` |
| `WebhookNotificationConfig` | `url`, `headers{}` *values* |
| `JDBCConfig` | `url`, `user`, `password`, `driver`, `properties{}` *values*, `from_secret` |

References in `custom_query.sql`, `filter`, `partition_ts`,
collector / validator fields, or anywhere outside the allowlist
**stay literal** — operators expect their SQL to render verbatim.

### Out of scope

- **Recipient lists** are not walked. `recipients: ["secret://..."]`
  stays literal. Recipients are addressees, not credentials, and the
  email notifier already prints them in INFO logs.
- **Programmatically-registered notifier instances** —
  `qf.register_notifier("prod", EmailNotifier(smtp_password="secret://..."))`
  passes a fully-built notifier object whose attributes are plain
  Python strings. Resolve before constructing.
- **Plain `${ENV_VAR}` interpolation is not supported.** Qualifire
  does not expand environment variables in YAML. Operators on
  non-AIDP platforms either pass already-resolved values to the
  Python API or run their own pre-load templating step.

### Resolver Protocol

Any object with `get(name, key=None) -> Any` works:

```python
class _MyResolver:
    def get(self, name, key=None):
        return ...  # talk to your vault here
```

`aidputils.secrets` already conforms. Vault / AWS Secrets Manager /
Azure Key Vault adapters fit in a 5-line wrapper.

### Errors

- Reference present, no resolver supplied →
  `MissingSecretResolverError` at the API entry that built the config
  (constructor-time for `Qualifire(jdbc=..., system_table_backend="jdbc")`,
  `run_config*` / `validate*` time for in-config references).
- Resolver raises (network blip, secret-not-found, permission denied)
  → `SecretResolutionError` naming the reference + field path; the
  original exception's *type name* is preserved but the original
  message and traceback are **dropped** (vault SDKs occasionally
  echo the value in error messages — we never echo whatever they say).
- Resolver returns a non-`str` for a string field → `SecretResolutionError`
  naming the expected vs. actual type, never the value.
