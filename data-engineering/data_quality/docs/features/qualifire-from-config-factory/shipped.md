---
shipped: 2026-05-09
---

# Shipped: Qualifire.from_config — Single-Source-of-Truth Constructor

## Summary

Adds `Qualifire.from_config(path, *, backend, **overrides)` classmethod
that constructs a fully-configured `Qualifire` instance from a YAML
config plus a runtime `backend`. Removes the unused YAML `backend:`
field. End state: a YAML-driven setup is two lines instead of seven.

```python
qf = Qualifire.from_config('configs/quality.yaml', backend=SparkBackend(spark))
qf.run_config('configs/quality.yaml', context={'ds': '2026-05-09'})
```

The direct `Qualifire(...)` constructor stays unchanged for purely
programmatic / no-YAML setups — operators supply `backend`,
`system_table`, `system_table_backend`, `owner`, `bu` (and optionally
`jdbc`, `notifiers`, `secret_resolver`, `warn_on_reinit`) explicitly.

## Key Changes

### Schema migration

- Dropped `backend: str = "spark"` from `QualifireConfig`. The field
  never drove runtime backend selection — purely a label.
- Added `@model_validator(mode="before")` that detects stale
  `backend:` and raises `QualifireConfigError` with a migration
  message. Pydantic's default extra-field-drop would otherwise let
  the field vanish silently on upgrade.

### API

- New `Qualifire.from_config(path, *, backend, **overrides)`
  classmethod factory. Lifts `owner`, `bu`, `system_table`,
  `system_table_backend`, `jdbc` from YAML onto the instance. Runtime
  `backend` is the only required Python kwarg. Accepts `backend=None`
  for the SQLite-only path.
- New module-private `Qualifire._from_parsed_config(config, ...)`
  classmethod for the CLI to avoid double `load_config()` reads (TOCTOU
  guard).
- New `_effective_notifiers(yaml_notifications=None)` helper used by
  all 5 engine entrypoints (`run_config_parsed`, `backfill`,
  `validate`, `validate_query`, `write_audit_publish`). Single
  notifier-precedence contract: programmatic instances ALWAYS win over
  YAML's `notifications:` block on every call.
- New `_swap_storage_if_needed(config)` helper centralizes the
  storage-swap reinit branch shared by `run_config_parsed` and
  `_resolve_storage_for_op`. Emits `QualifireReinitWarning` when
  `warn_on_reinit=True` and any of `system_table` /
  `system_table_backend` / `jdbc` differ.
- New `QualifireReinitWarning` (subclass of `UserWarning`), exported
  from `qualifire.__init__`.
- `warn_on_reinit=True` is the unified default for both `from_config`
  and direct `Qualifire(...)`. Pass `False` to silence intentional
  swaps (CLI does this).

### CLI

- `_cmd_run`, `_cmd_report`, `_open_qualifire_for_op` (used by
  backfill + deactivate-metric) all switch to
  `Qualifire._from_parsed_config(parsed, backend=..., warn_on_reinit=False)`.
  Single `load_config` call per CLI invocation; TOCTOU-clean.

### YAML migration

- Dropped `backend:` from 13 YAMLs (8 in `docs/examples/`, 3 in
  `examples/industries/`, 2 in `tests/manual/configs/`) and 2 doc YAML
  snippets (`docs/configuration.md`, `docs/getting_started.md`).
- Acceptance grep `grep -rn '^backend:' docs/ examples/ tests/
  --exclude-dir=features` is empty.

### Docs

- New "Constructing from YAML" section in `docs/programmatic_api.md`.
- New `docs/CHANGELOG.md` documenting the breaking change with a
  migration diff.
- README + `getting_started.md` + `configuration.md` updated to show
  the factory pattern and dict-form `expressions:` in
  threshold/drift/trend examples (the legacy list-form is no longer
  accepted by Pydantic).

## Files Changed

- `qualifire/__init__.py` — export `QualifireReinitWarning`
- `qualifire/api.py` — factory + helpers + reinit warning wiring
- `qualifire/cli.py` — three call sites switched to
  `_from_parsed_config`
- `qualifire/core/config.py` — schema removal + migration validator
- `qualifire/core/exceptions.py` — `QualifireReinitWarning`
- `tests/test_api_from_config.py` (new, 29 tests)
- `tests/test_cli.py` — 4 single-load TOCTOU guards
- `tests/test_config.py` — schema-migration tests + drop legacy
  assertion
- 13 YAML files — `backend:` line removed
- `docs/configuration.md`, `docs/getting_started.md`,
  `docs/programmatic_api.md`, `README.md`, `docs/CHANGELOG.md`

## Testing

- Full `pytest -q` green: **1243 passed**.
- 29 new tests in `tests/test_api_from_config.py` covering minimum
  signature, override precedence, notifier always-wins (engine handoff
  for all 5 entrypoints + `backfill` driver handoff), reinit warnings
  (run_config + backfill + deactivate_metric public paths),
  zero-double-init, JDBC-eager / notification-lazy secret-resolution
  split, `_from_parsed_config` parity, `register_notifier`
  post-construction, backfill TOCTOU regression guard.
- 4 new CLI single-load TOCTOU guards (run, report, backfill,
  deactivate-metric).

## Review Iterations

- **Plan reviews**: 2 adversarial self-review rounds + 2 codex
  rounds. Codex plan-R1 FAIL (3 MUST-FIX) → R2 CONDITIONAL PASS (2
  MUST-FIX) → all addressed.
- **Implementation reviews**: 2 adversarial self-review rounds + 2
  codex rounds. Codex impl-R1 FAIL (2 MUST-FIX: impl uncommitted +
  notebook noise) → R2 CONDITIONAL PASS (1 MUST-FIX: real `backfill`
  TOCTOU bug discovered + fixed) → all addressed.

## Notes

- Manual notebooks (`tests/manual/notebook_*.ipynb`) intentionally
  not modified — flagged for operator-side update on a follow-up.
- The simplified design (single notifier layer; unified
  `warn_on_reinit=True` default) was a deliberate post-codex-R2 pivot
  per project directive that backwards-compat shims aren't needed —
  breaking changes can be enforced when a cleaner contract pays off.
