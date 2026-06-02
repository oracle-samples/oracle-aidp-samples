---
id: qualifire-from-config-factory
name: Qualifire.from_config — Single-Source-of-Truth Constructor
type: Enhancement
priority: P2
effort: Small
impact: Medium
created: 2026-05-08
---

# Qualifire.from_config — Single-Source-of-Truth Constructor

## Problem Statement

Operators wiring up Qualifire today have to declare the same identity
fields **twice** — once in the YAML config, once in the constructor:

```python
# Constructor side (Python)
qf = Qualifire(
    backend=SparkBackend(spark),
    system_table=str(SYSTEM_DB),
    system_table_backend='sqlite',
    owner='data-quality-demo',
    bu='demo',
)
qf.run_config('configs/end_to_end_sources.yaml', context={'ds': iso})
```

```yaml
# YAML side (same fields, restated)
owner: "data-quality-demo"
bu: "demo"
system_table: "{{ system_db }}"
system_table_backend: "sqlite"
backend: "spark"
notifications: { ... }
```

Five constructor kwargs (`owner`, `bu`, `system_table`,
`system_table_backend`, `notifications`) are duplicated against the
YAML's top-level fields. The friction this causes:

1. **Drift between Python and YAML.** Operators sometimes update one
   side and forget the other. The mismatch is silent — the first
   `run_config` call's reinit logic swaps the storage handle to match
   the YAML, but `owner`/`bu` on already-emitted log lines or
   diagnostics from before that point reflect the constructor values.
2. **Verbose notebooks and example scripts.** Every demo,
   smoke-test notebook, and quickstart that uses YAML duplicates the
   five fields. The end-to-end demo notebook is a representative
   example; so are `tests/manual/notebook_overall.ipynb` and the
   industry-pack quickstarts.
3. **YAML is already the canonical source.** `run_config_parsed`
   *already* knows how to reinit `_storage` when YAML's
   `system_table` / `system_table_backend` / `jdbc` differ from
   instance state. The constructor is effectively seeding defaults
   that the first `run_config` call overrides anyway.

## Proposed Solution

A classmethod factory whose minimum signature is **just (path,
backend)**. Everything else lives in YAML.

```python
qf = Qualifire.from_config(
    'configs/end_to_end_sources.yaml',
    backend=SparkBackend(spark),                       # required
    notifiers={'inbox': CapturingNotifier()},          # optional
    secret_resolver=...,                               # optional
    **overrides,                                       # any field overrides YAML
)
```

Two coupled design moves to make this work cleanly:

**1. Drop the YAML `backend:` field.** The runtime backend
(`SparkBackend(spark)`, `PandasBackend()`, etc.) is the live
in-process object — it can't round-trip through YAML. The current
`backend: "spark"` field is purely a label that does nothing
useful at runtime; it was a placeholder that the engine never
actually consumed for backend selection. Remove it from the
`QualifireConfig` schema entirely. No compat shim — operators
update their YAMLs and pass the backend explicitly. Every YAML
example in the repo loses one line.

**2. Everything else flows from YAML.** `from_config` reads
`owner`, `bu`, `system_table`, `system_table_backend`, `jdbc`,
`notifications`, `context`, and `datasets` straight from the
loaded config. Builds notifiers from the YAML `notifications:`
block. Constructs the storage handle from
`system_table_backend` + `system_table` (+ `jdbc` if present).

Behaviour:
- `backend` is the only required runtime kwarg.
- Programmatic `notifiers={}` merge over YAML notifiers, programmatic
  wins on name collision (matches the existing precedence rule in
  `_build_notifiers`).
- Any explicit constructor kwarg overrides the corresponding YAML
  field. Operator who passes `owner='override'` to `from_config`
  gets that, even if the YAML says `owner: "demo"`.
- Identical end-state to today's `Qualifire(...)` followed by
  `qf.run_config_parsed(config)` reinit — so subsequent
  `run_config` calls are zero-cost (no double reinit).
- Existing `Qualifire(...)` constructor stays. `from_config` is
  additive on top of it; the existing constructor is the
  "no YAML" path for purely programmatic callers.

## Why this matters

- **Removes a recurring footgun.** Constructor / YAML drift is the
  #1 question operators run into when adopting the framework. Folding
  it into a single source of truth eliminates an entire class of bug.
- **Quickstarts and notebooks shrink.** The five-line constructor
  block becomes a one-liner. Every example in `examples/`,
  `tests/manual/`, and `docs/` benefits.
- **Consistent with how other config-driven libs ship.** dbt, Airflow,
  and Spark itself all support "load profile / load conf" factory
  methods. Qualifire's `Qualifire(...)` + `run_config(...)` two-step
  is an outlier.

## Affected Areas

- `qualifire/api.py` — new `Qualifire.from_config` classmethod;
  small refactor of `__init__` so factory and direct construction
  share the storage-init path.
- `qualifire/cli.py` — internal helper `_build_qualifire(args,
  config)` already does this manually; collapse into
  `Qualifire.from_config`.
- `docs/programmatic_api.md` — new section "Constructing from
  YAML" with the recommended idiom.
- `docs/configuration.md` — note that `system_table_backend` /
  `notifications` etc. flow into the instance when using
  `from_config`.
- `README.md` — replace the canonical "first ten lines" example.
- `examples/backfill_quickstart.py` (when shipped) and the
  industry-pack quickstarts.
- `tests/test_api.py` — coverage for `from_config` path,
  including kwarg-overrides-YAML and notifier merge precedence.

## Captured Constraints (decision pins for the plan)

1. **YAML `backend:` field is removed from the schema.** Not
   deprecated, not warned-on, removed. The field never drove
   runtime behaviour; keeping it would just preserve a footgun.
   Every YAML example in the repo (`docs/examples/*.yaml`,
   `examples/industries/*/config.yaml`,
   `tests/manual/configs/*.yaml`, test fixtures) drops the line.
   Operators upgrading drop it from their own YAMLs in the same
   commit they upgrade.
2. **`backend` is the one required runtime kwarg on `from_config`.**
   The live `SparkSession` / pandas runtime / JDBC pool can't
   round-trip through YAML; that's the only thing the operator
   has to supply at construction.
3. **Programmatic kwargs win over YAML on collision.** Same
   precedence rule as today's
   `_notifiers.update(_build_notifiers(config.notifications))`
   in `run_config_parsed`. Documented explicitly so operators know
   how to override.
4. **No Jinja rendering of top-level scalars.** `load_config`
   doesn't Jinja-render `system_table` and friends today;
   `from_config` inherits that. Operators using `{{ ... }}` in
   those fields keep the existing pattern — substitute textually
   before calling `from_config`. Documented in the new section.
5. **Multi-YAML usage stays unchanged.** Operators running multiple
   YAMLs against one instance (the demo notebook does — sources
   YAML and WAP YAML) pick one as the construction source; the
   second YAML's `run_config` triggers reinit only if it disagrees
   on `system_table` / `system_table_backend` / `jdbc`. Optional
   warn-on-disagreement when the second YAML disagrees is in scope
   for this feature — operators almost never want silent reinit
   under a single notebook run.
6. **Existing `Qualifire(...)` constructor stays.** No deprecation,
   no compat shim churn. `from_config` is purely additive — the
   programmatic-only path (no YAML) keeps working unchanged.

## Open Questions (for the plan phase)

- **`secret_resolver` resolution timing.** Today the constructor
  accepts a `SecretResolver` instance; YAML can reference secrets
  via `secret://` and `from_secret:`. Should `from_config` resolve
  secrets eagerly (using a resolver passed as kwarg) or lazily
  (deferred to first `run_config` call)? Lean lazy — matches
  current behaviour.
- **CLI parity.** Should the CLI's `_build_qualifire` switch to
  `Qualifire.from_config` internally, or stay separate? Lean
  toward switching — eliminates the second copy of the same logic.
  Plan should land both at once.

## Notes / Captured Constraints

- Captured 2026-05-08 from a notebook-walkthrough discussion: the
  constructor's five duplicated kwargs were a recurring annoyance
  in the end-to-end demo and earlier `notebook_overall.ipynb`.
- Pairs with `metrics-backfill-and-soft-delete` (the new
  `qf.backfill(...)` API would also benefit from the simpler
  construction story) but is independently shippable. The two
  features don't need to land in either order.
