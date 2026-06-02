---
started: 2026-05-08
updated: 2026-05-09
---

# Implementation Plan: Qualifire.from_config — Single-Source-of-Truth Constructor

## Overview

Add a `Qualifire.from_config(path, *, backend, **overrides)` classmethod
that constructs a fully-configured `Qualifire` instance from a YAML
config plus a runtime `backend`. Drop the YAML `backend:` field from
the schema. Migrate every YAML example, every internal call site
(three of them in `cli.py`), the example script, and add a Pydantic-
level migration error for stale `backend:` fields.

End state: the canonical "first three lines" of a Qualifire-using
script is

```python
from qualifire.api import Qualifire
from qualifire.backends.spark_backend import SparkBackend

qf = Qualifire.from_config('configs/quality.yaml', backend=SparkBackend(spark))
qf.run_config('configs/quality.yaml', context={'ds': '2026-05-08'})
```

(Same path for both calls is the common case; advanced users can
construct from one YAML and run a different one — `run_config_parsed`'s
existing reinit logic handles the divergence and now warns by default
when `from_config` constructed the instance.)

## Drift Audit (vs. current `main`, 2026-05-09)

The plan was written 2026-05-08 against the pre-`backfill-api-and-wap-target-mode`
state of `main`. Re-audit findings:

1. **No code reads `config.backend`.** `grep -rn "config\.backend"
   qualifire/ tests/` returns zero. Step 1.2's "remove references"
   list is empty — schema removal alone is sufficient on the code
   side. (Step 1.2 collapsed.)
2. **`QualifireConfig` has no `extra='forbid'`.** Pydantic v2 default
   is to silently drop unknown fields. Plan's claim that "leftover
   `backend:` will error at `load_config()` time" is wrong without
   intervention. Fix: add a `@model_validator(mode="before")` that
   detects `backend:` specifically and raises
   `QualifireConfigError` with the migration message. Adding global
   `extra='forbid'` is out of scope (would surface unrelated stale
   fields and break working YAMLs).
3. **CLI `_build_qualifire` doesn't exist.** Real targets:
   - `qualifire/cli.py:_cmd_run` — inline `Qualifire(...)` block at L203.
   - `qualifire/cli.py:_cmd_report` — inline `Qualifire(...)` block at L279.
   - `qualifire/cli.py:_open_qualifire_for_op` — used by
     `_cmd_backfill` and `_cmd_deactivate_metric`, L363.
   All three duplicate the YAML-fields-into-constructor pattern; all
   three migrate to `Qualifire.from_config`.
4. **`examples/backfill_quickstart.py`** carries the long-form
   constructor (L53-L60). Plan didn't list it. Migrate.
5. **Notifier precedence drift.** idea.md captured constraint #3
   says "programmatic wins", but the existing
   `run_config_parsed`/`validate`/`backfill` code does
   `notifiers = dict(self._notifiers); notifiers.update(yaml)` —
   YAML wins. Resolution pinned in Step 2 below.
6. **`warn_on_reinit` default.** Single contract across every
   construction path: `warn_on_reinit=True` defaults for both
   `from_config` and direct `Qualifire(...)`. Storage swaps now
   surface to operators by default; pass `warn_on_reinit=False`
   to silence intentional swaps (CLI does this because
   construction and execution use the same YAML). No legacy
   silent mode is preserved (per project directive — we can
   enforce breaking changes when needed).
7. **YAML migration scope.** `grep -rn '^backend:' docs/
   examples/ tests/ --exclude-dir=features` returns 15 hits
   across 15 files (the historical idea.md inside
   `docs/features/qualifire-from-config-factory/` is not in
   scope — it documents the before-state). Full file list in
   Step 3.

## Scope Gates / Hard Stops

- **In scope:** new factory classmethod, schema removal of
  `QualifireConfig.backend`, programmatic kwarg precedence (pinned),
  multi-YAML warn-on-disagreement (instance-flag-controlled), CLI
  three-call-site migration, `examples/backfill_quickstart.py`,
  every YAML in repo, docs, tests.
- **Out of scope:**
  - Adding global `extra='forbid'` to `QualifireConfig` (only the
    `backend` migration validator).
  - Auto-instantiating the backend from a YAML string identifier
    (deliberately abandoned per idea Captured Constraint #2).
  - Changes to `run_config_parsed`'s reinit semantics beyond the
    new `_warn_on_reinit` warning.
  - Manual notebooks (`tests/manual/notebook_*.ipynb`) — flagged
    for operator-side update; not modified by this PR (consistent
    with prior shipped features).
  - Changes to per-config fields (`notifications`,
    `engine_notify`, `partition_ts`, `partition_step`,
    `dataset_parallelism`, `context`, `datasets`) —
    `from_config` only handles **instance-level** fields
    (owner, bu, system_table, system_table_backend, jdbc). The
    `notifications:` block is treated as per-config: each
    `run_config_parsed` / `backfill` / `validate(config=...)`
    call merges YAML notifications for that call. Programmatic
    paths (`validate_query`, `write_audit_publish`, no-config
    `validate`) only see `self._notifiers` (the single-layer
    notifier dict).
- **Hard stops:**
  - If schema removal + migration validator breaks any existing test
    YAML (excluding the ones we migrate in Step 3), surface the
    failure loudly — no silent compat shim.
  - If a notifier-precedence test inversion is detected
    (programmatic value lost when YAML defines same name), block
    until precedence is documented + tested before merge.

## Implementation Steps

### Step 1 — Drop `backend` from `QualifireConfig`; add migration validator

- [ ] **1.1** Remove the line
  `backend: str = "spark"` from `qualifire/core/config.py`
  (currently L1091).
- [ ] **1.2** Add a `@model_validator(mode="before")` on
  `QualifireConfig` that detects a stale `backend:` field in the
  raw input dict and raises `QualifireConfigError` with the message:

  ```
  YAML field `backend:` is no longer supported. The runtime backend
  is now passed directly to Qualifire.from_config(...) (or
  Qualifire(backend=...) for programmatic-only setups). Drop the
  `backend:` line from your YAML.
  ```

  Mode `"before"` runs against the raw input before schema
  validation so it sees fields not declared on the model.
  Implementation guards on `isinstance(data, dict)` so calls like
  `QualifireConfig.model_validate(existing_config_instance)` (input
  is already a model) skip the check:

  ```python
  @model_validator(mode='before')
  @classmethod
  def _reject_legacy_backend_field(cls, data):
      if isinstance(data, dict) and 'backend' in data:
          raise QualifireConfigError(
              "YAML field `backend:` is no longer supported. The "
              "runtime backend is now passed directly to "
              "Qualifire.from_config(...) (or "
              "Qualifire(backend=...) for programmatic-only "
              "setups). Drop the `backend:` line from your YAML."
          )
      return data
  ```

  Sibling validators on `QualifireConfig` (e.g.
  `_validate_partition_step` with `mode='after'`) won't conflict.
- [ ] **1.3** Verify no remaining reads of `config.backend` (grep
  showed zero on the audit; re-grep after removal as a safety net):
  ```bash
  grep -rn "config\.backend\b" qualifire/ tests/
  ```
  Expected: empty.
- [ ] **1.4** Tests: `tests/test_config.py` — assert that loading
  a YAML containing `backend: "spark"` raises
  `QualifireConfigError` whose message contains `Qualifire.from_config`.

**Exit criteria for Step 1**: schema rejects `backend:` with the
documented migration message; full repo grep confirms no live reads.

### Step 2 — Add `Qualifire.from_config` classmethod

Implement in `qualifire/api.py` directly under `__init__`:

```python
@classmethod
def from_config(
    cls,
    config_path: str | Path,
    *,
    backend: Backend | None,
    notifiers: dict[str, Notifier] | None = None,
    secret_resolver: SecretResolver | None = None,
    warn_on_reinit: bool = True,
    **overrides: Any,
) -> Qualifire:
    """Construct a Qualifire instance from a YAML config + a
    runtime backend.

    Reads ``owner``, ``bu``, ``system_table``,
    ``system_table_backend``, and ``jdbc`` from the loaded
    config and lifts them onto the instance. Per-config fields
    (``notifications``, ``engine_notify``, ``partition_ts``,
    ``partition_step``, ``dataset_parallelism``, ``context``,
    ``datasets``) stay attached to each run's config and apply
    per ``run_config_parsed`` / ``backfill`` / ``validate``
    (when the call is YAML-backed) — they are NOT lifted onto
    the instance. To make a notifier visible to programmatic
    paths (``validate_query``, ``write_audit_publish``,
    ``validate(...)`` without a config), pass it as a runtime
    instance via the ``notifiers={}`` kwarg; that becomes a
    permanent override that wins on every call.

    Any of ``owner``, ``bu``, ``system_table``,
    ``system_table_backend``, ``jdbc`` passed as ``**overrides``
    wins over the YAML value. ``notifications`` is **not** an
    accepted override key (it's a dict and the merge semantics
    would be ambiguous); use the explicit ``notifiers={}``
    runtime-instance kwarg instead. Unknown override keys raise
    ``TypeError`` so typos surface immediately rather than being
    silently dropped.

    The live ``backend`` instance is the only required runtime
    kwarg; YAML cannot carry a ``SparkSession`` / JDBC pool /
    pandas runtime. ``backend=None`` is accepted for the SQLite-
    only path where storage operations don't need a Spark session
    (mirrors the existing ``Qualifire(backend=None,
    system_table_backend="sqlite", ...)`` pattern used by the CLI's
    ``_open_qualifire_for_op``).

    Notifier merge precedence is single-layer: programmatic
    notifiers (``notifiers={}`` passed to ``from_config`` or
    ``Qualifire(...)``, plus any post-construction
    ``register_notifier`` calls) flow into ``self._notifiers``
    and ALWAYS win over a YAML ``notifications:`` block of the
    same name on every engine call (``run_config_parsed``,
    ``backfill``, ``validate``, ``validate_query``,
    ``write_audit_publish``). The factory does NOT pre-build
    YAML notifiers; ``secret://`` references inside the YAML
    ``notifications:`` block stay deferred and resolve
    per-call. A test harness that passes
    ``notifiers={'inbox': CapturingNotifier()}`` therefore
    keeps that capture in effect across multiple
    ``run_config`` calls, even when later YAMLs declare an
    ``inbox`` of a different type. Same contract for direct
    ``Qualifire(notifiers={...})`` — programmatic always wins.

    ``warn_on_reinit=True`` (default) emits
    ``QualifireReinitWarning`` when a subsequent
    ``run_config_parsed`` / ``backfill`` /
    ``deactivate_metric`` call's YAML disagrees with the
    construction-time YAML on ``system_table`` /
    ``system_table_backend`` / ``jdbc``. Pass ``False`` to
    silence the warning when the swap is intentional (e.g.
    CLI paths). The default applies to every construction
    path — one consistent contract across ``from_config`` and
    direct ``Qualifire(...)``.

    Secret resolution timing matches existing semantics, NOT a
    blanket "lazy" rule:
    - **JDBC connection secrets resolve eagerly** at
      construction (storage opens during ``__init__``;
      ``Qualifire.__init__`` already resolves
      ``jdbc.secret://`` refs before
      ``open_storage`` — this path is preserved).
    - **YAML ``notifications:`` block secrets resolve lazily**
      (per-call). Because ``from_config`` does NOT pre-build
      YAML notifiers, ``secret://`` refs inside
      ``notifications:`` are never read at construction time;
      ``_resolve_secrets_or_raise`` inside
      ``run_config_parsed`` / ``backfill`` resolves them
      before notifier instantiation, exactly as today.
    - The ``secret_resolver`` kwarg is held on the instance
      and used by both paths.

    Raises:
        QualifireConfigError: if the file is missing, malformed,
            or carries the deprecated ``backend:`` field.
    """
```

- [ ] **2.1** Implement the body. Reuse `load_config` for parsing
  (this triggers Step 1.2's `backend:` migration validator
  automatically). Lift the five instance-level fields
  (`owner`, `bu`, `system_table`, `system_table_backend`, `jdbc`),
  apply `**overrides`, hand straight to `cls(...)` along with
  the `notifiers={...}` kwarg (which flows into `self._notifiers`).
  YAML `notifications:` configs are NOT pre-built — they remain
  attached to each per-call config so `secret://` references
  resolve at run time. Single source of truth for notifier
  resolution lives in the new `_effective_notifiers` helper
  (Step 2.4).
- [ ] **2.2** Refactor `__init__` only as needed: add a public
  kwarg-only `warn_on_reinit: bool = True` to `__init__`, stored
  on `self._warn_on_reinit`. Default True applies to every
  construction path — one consistent contract across
  `from_config` and direct `Qualifire(...)`. No legacy silent
  mode. Operators who want silence pass `warn_on_reinit=False`
  explicitly (CLI does this because construction and execution
  use the same YAML).
- [ ] **2.3** Add `QualifireReinitWarning` class (subclass of
  `UserWarning`) in `qualifire/core/exceptions.py`. **Centralize**
  the reinit warning so it fires from EVERY storage-swap branch,
  not just `run_config_parsed`. Today there are two duplicated
  storage-swap branches:
  - `run_config_parsed` (lines 382-411) — used by
    `qf.run_config(...)`.
  - `_resolve_storage_for_op` (lines 632-657) — used by
    `qf.backfill(...)`, `qf.deactivate_metric(...)`.

  Extract a shared `_swap_storage_with_warning(config)` helper
  that both call. The helper computes `table_changed /
  backend_changed / jdbc_changed`, emits the warning when any
  dimension flips and `self._warn_on_reinit` is True, and
  performs the atomic candidate-locals + commit pattern.

  ```python
  def _swap_storage_if_needed(self, config: QualifireConfig) -> Any:
      """Shared storage-swap path. Used by run_config_parsed and
      _resolve_storage_for_op so both honor warn_on_reinit."""
      table_changed = bool(config.system_table) and config.system_table != self.system_table
      backend_explicit = "system_table_backend" in config.model_fields_set
      backend_changed = backend_explicit and config.system_table_backend != self.system_table_backend
      jdbc_explicit = "jdbc" in config.model_fields_set
      jdbc_changed = jdbc_explicit and config.jdbc != self.jdbc

      if not (table_changed or backend_changed or jdbc_changed):
          return self._storage

      candidate_table = config.system_table
      candidate_backend = config.system_table_backend if backend_explicit else self.system_table_backend
      candidate_jdbc = config.jdbc if jdbc_explicit else self.jdbc

      if self._warn_on_reinit:
          changes = []
          if table_changed:
              changes.append(f"system_table: {self.system_table!r} -> {candidate_table!r}")
          if backend_changed:
              changes.append(f"system_table_backend: {self.system_table_backend!r} -> {candidate_backend!r}")
          if jdbc_changed:
              changes.append("jdbc: <changed>")
          warnings.warn(
              QualifireReinitWarning(
                  f"reinitializing storage: {'; '.join(changes)}. "
                  f"Pass warn_on_reinit=False to from_config(...) "
                  f"if this storage swap is intentional."
              ),
              stacklevel=3,  # one extra frame because of the helper
          )

      storage = self._init_storage(
          system_table=candidate_table,
          system_table_backend=candidate_backend,
          jdbc=candidate_jdbc,
      )
      self.system_table = candidate_table
      self.system_table_backend = candidate_backend
      self.jdbc = candidate_jdbc
      self._storage = storage
      return storage
  ```

  Both `run_config_parsed` and `_resolve_storage_for_op` collapse
  their reinit branches to `self._swap_storage_if_needed(config)`.
  No reference to "prior YAML" in the message — the instance does
  not retain its construction-time YAML path. The before/after
  values are sufficient for the operator to identify the source.
- [ ] **2.4** Single notifier layer: programmatic notifiers
  (constructor `notifiers={}` from either `from_config` or direct
  `Qualifire(...)`, plus `register_notifier(...)` writes) flow
  into `self._notifiers`. **`from_config` does NOT eagerly build
  YAML notifiers** — that defers all `secret://` resolution in
  `notifications:` blocks to per-call time.

  Centralize notifier assembly in a new helper that every engine
  call site uses. Replaces the five direct `self._notifiers`
  passes scattered today (`run_config_parsed`, `backfill`,
  `validate`, `validate_query`, `write_audit_publish`):

  ```python
  def _effective_notifiers(
      self,
      yaml_notifications: dict[str, NotificationChannelConfig] | None = None,
  ) -> dict[str, Notifier]:
      """Single source of truth for notifier resolution.

      Merge order (later wins):
        1. YAML ``notifications:`` block (if a config is in
           scope) — built lazily via ``_build_notifiers``.
        2. ``self._notifiers`` — programmatic instances always
           win (one consistent contract).
      """
      merged = {}
      if yaml_notifications:
          merged.update(self._build_notifiers(yaml_notifications))
      merged.update(self._notifiers)
      return merged
  ```

  Migrate all five call sites:
  - `run_config_parsed` and `backfill`:
    `notifiers = self._effective_notifiers(config.notifications)`.
  - `validate`, `validate_query`, `write_audit_publish`:
    `notifiers=self._effective_notifiers()` (no YAML in scope).

  `register_notifier(name, instance)` writes to `self._notifiers`
  and thus participates in the always-wins layer. Operators can
  swap notifiers post-construction and the swap survives every
  subsequent `run_config` call.

**Exit criteria for Step 2**: `from_config` round-trips against the
end-to-end demo's YAMLs (after Step 3 migration); reinit-warning
fires from every storage-swap path; programmatic notifiers always
win over YAML.

### Step 3 — Migrate every YAML in the repo

`backend:` removal hits every example. Verified live: `grep -rn
'^backend:' docs/ examples/ tests/` returns 17 hits across 14 files.

- [ ] **3.1** `docs/examples/*.yaml` — remove `backend:` from
  `anomaly_detection.yaml`, `forecast_check.yaml`,
  `full_config.yaml`, `historical_check.yaml`, `profiling_check.yaml`,
  `slo_check.yaml`, `threshold_check.yaml`, `wap_pattern.yaml`
  (8 files).
- [ ] **3.2** `examples/industries/{financial_services,life_sciences,retail}/config.yaml`
  (3 files).
- [ ] **3.3** `tests/manual/configs/end_to_end_sources.yaml` and
  `end_to_end_wap.yaml` (2 files).
- [ ] **3.4** `docs/configuration.md:10` and
  `docs/getting_started.md:202` — these are YAML examples embedded
  inside markdown fenced code blocks (not standalone YAML files).
  Edit the markdown: drop the `backend:` line from the embedded
  YAML and update surrounding prose to show the
  `Qualifire.from_config(...)` idiom for wiring the runtime
  backend.

**Exit criteria for Step 3**: `grep -rn '^backend:' docs/
examples/ tests/ --exclude-dir=features` returns empty. The
historical idea.md at
`docs/features/qualifire-from-config-factory/idea.md:36` carries
a `backend: "spark"` snippet inside its problem-statement code
fence — that's the documented before-state and stays. The
`features/` exclusion is the standard way to scope the grep to
live YAMLs only.

### Step 4 — Migrate internal Python call sites

- [ ] **4.1** `qualifire/cli.py:_cmd_run` — replace inline
  `Qualifire(...)` block (L203) with the factory. The CLI already
  loads the parsed config once for preflight; we must avoid a
  second `load_config()` read (TOCTOU window). Decision: **add a
  module-private `Qualifire._from_parsed_config(config, *, backend,
  ...)` classmethod** (single leading underscore, internal API
  by Python convention) that takes an already-parsed
  `QualifireConfig`. `from_config(path, ...)` calls
  `load_config(path)` then delegates to `_from_parsed_config`. The
  CLI uses `_from_parsed_config`. Single-underscore is the deliberate
  signal: "stable enough that we test it; not part of the documented
  public API surface yet."

  CLI call becomes:
  ```python
  qf = Qualifire._from_parsed_config(
      config,
      backend=SparkBackend(spark),
      warn_on_reinit=False,
  )
  ```
  `warn_on_reinit=False` because the CLI loads the same YAML for
  both construction and `run_config_parsed`, so the warning would
  never fire usefully — and the CLI's stderr is meant for
  operator-relevant signals only.
- [ ] **4.2** `qualifire/cli.py:_cmd_report` — same migration as 4.1.
- [ ] **4.3** `qualifire/cli.py:_open_qualifire_for_op` — collapse
  to a `Qualifire._from_parsed_config(config, backend=..., warn_on_reinit=False)`
  call (the function already has the parsed config in hand).
  Backfill / deactivate-metric commands use this; the SparkSession-
  fallback logic (try Spark, accept None for SQLite-only) lives
  *outside* the construction call so the operator-facing error
  message stays unchanged.
- [ ] **4.4** `examples/backfill_quickstart.py` (L53-L60) — replace
  the long-form constructor with `Qualifire.from_config`. Note: the
  example currently constructs from kwargs alone (no YAML); to
  migrate to `from_config` the example needs a paired YAML. Two
  alternatives:
  - 4.4a: Keep the example using `Qualifire(...)` (it's an example
    of the programmatic-only path and should stay that way).
  - 4.4b: Add a sibling YAML and migrate.
  Lean 4.4a — programmatic example shouldn't require a YAML.
  Document `from_config` in a separate `docs/examples/` snippet
  instead.
- [ ] **4.5** `README.md` "first ten lines" example — replace
  whatever construction idiom it shows with the `from_config`
  pattern. (Re-grep at impl time.)

**Exit criteria for Step 4**: every YAML-driven entry point uses
the factory; programmatic-only entry points (no YAML) keep using
`Qualifire(...)`.

### Step 5 — Tests

- [ ] **5.1** New `tests/test_api_from_config.py`:
  - `test_from_config_minimum` — only `(path, backend)`; assert
    instance owner / bu / system_table / system_table_backend /
    jdbc come from YAML.
  - `test_from_config_overrides_win` — `owner='override'` kwarg
    wins over YAML's `owner: "demo"`.
  - `test_from_config_notifiers_merge_programmatic_wins` —
    programmatic `notifiers={'inbox': CapturingNotifier()}` over
    a YAML that defines `inbox` as e.g. `type: slack`; assert
    `qf._notifiers["inbox"] is captured` (single-layer notifier
    dict gets the runtime instance).
  - `test_programmatic_wins_at_engine_handoff` — after
    `from_config(yaml, notifiers={'inbox': captured})`, calling
    `qf.run_config(yaml)` (which YAML-merges `notifications`)
    results in the engine seeing `captured`, not the YAML's
    Slack notifier. Verifies the always-wins semantic at the
    engine-handoff point by spying on `QualifireEngine` kwargs.
  - `test_from_config_unknown_backend_field_errors` — YAML with
    `backend: "spark"` raises `QualifireConfigError` with a
    message containing `Qualifire.from_config`.
  - `test_from_config_reinit_warns_on_run_config` — second
    `run_config` with a different `system_table` triggers
    `QualifireReinitWarning` when constructed via
    `from_config(..., warn_on_reinit=True)` (the default);
    silent when `warn_on_reinit=False`.
  - `test_from_config_reinit_warns_on_backfill_and_deactivate`
    — codex R2 MUST-FIX 2 regression guard:
    `qf.backfill(other_yaml_with_diff_table, ...)` and
    `qf.deactivate_metric(other_yaml_with_diff_table, ...)`
    each emit `QualifireReinitWarning` (proves both
    `_resolve_storage_for_op`-using paths are wired into
    `_swap_storage_if_needed`).
  - `test_direct_init_warns_on_reinit_by_default` — single-
    contract guard: `Qualifire(...)` defaults to
    `warn_on_reinit=True` like every other path. A storage
    swap fires the warning. (No legacy silent mode.)
  - `test_from_config_zero_double_init` — assert in two
    phases (codex R1 SHOULD-FIX 4): bare `wraps=` on an unbound
    method drops `self`, so use a binding-safe spy. Two valid
    options:
    (i) `unittest.mock.patch.object(Qualifire, "_init_storage",
    autospec=True, side_effect=lambda self, **kw:
    real_init_storage(self, **kw))` — autospec preserves the
    signature; the lambda restores binding.
    (ii) Construct the instance, then wrap the bound method:
    `mock = MagicMock(wraps=qf._init_storage); qf._init_storage =
    mock`. Construct the instance after the patch context for
    option (i), or construct first and then wrap for option (ii).
    Assert in two phases: (a) immediately after
    `from_config(yaml)`, `mock.call_count == 1`; (b) after
    `qf.run_config(yaml)` on the same path,
    `mock.call_count == 1` still (no reinit). This proves the
    reinit branch did NOT call `_init_storage`.
  - `test_from_config_notification_secrets_lazy` — YAML's
    `notifications:` block contains `secret://...` references;
    `from_config(yaml, secret_resolver=resolver)` does NOT
    resolve them at construction (the `notifications:` block is
    not pre-built into notifiers; refs stay as raw strings on
    `config.notifications`). The first
    `run_config_parsed(config)` call resolves them via
    `_resolve_secrets_or_raise` before `_build_notifiers` runs.
    Verify by spying on `resolver.resolve` and asserting it's
    not called inside `from_config(...)`.
  - `test_from_config_jdbc_secrets_eager` — codex R2 SHOULD-FIX
    1 split: when `system_table_backend == "jdbc"` and the YAML
    `jdbc:` block contains `secret://`, `from_config` resolves
    them eagerly during construction (existing
    `Qualifire.__init__` JDBC path is preserved through
    `cls(...)`). Verify by spying on `resolver.resolve` and
    asserting it IS called during `from_config(...)`.
  - `test_from_parsed_config_path` — the module-private
    `Qualifire._from_parsed_config(config, backend=...)` accepts
    an already-parsed object (used by the CLI to avoid double
    `load_config` reads); identity assertions on the resulting
    instance match `from_config(path)` on the same YAML.
  - `test_direct_init_with_notifiers_kwarg_wins_over_yaml` —
    direct `Qualifire(notifiers={'inbox': captured})` followed
    by `qf.run_config(yaml_with_inbox_slack)` results in the
    engine seeing `captured`, not YAML's slack. Single contract:
    programmatic always wins regardless of construction path.
  - `test_overrides_apply_to_all_engine_entrypoints` — codex R1
    MUST-FIX 1 + R2 SHOULD-FIX 3 regression guard:
    `from_config(notifiers={'inbox': captured})` is visible
    to `_effective_notifiers()` calls from every engine
    entrypoint. Backed by a static-grep guard
    (`test_no_raw_self_notifiers_left_in_engine_paths`) that
    fails if any future code adds a raw
    `notifiers=self._notifiers` handoff bypassing the helper.
  - `test_register_notifier_post_construction_wins` — single-
    layer simplification: `register_notifier(name, instance)`
    writes to `self._notifiers` (the always-wins layer), so
    calling `register_notifier('inbox', replacement)` after
    `from_config(yaml, notifiers={'inbox': original})`
    REPLACES `original`. The next merge resolves `inbox` to
    `replacement`.
- [ ] **5.2** `tests/test_config.py` — two changes:
  - Remove the existing
    `assert cfg.backend == "spark"` at L95 (the field is gone).
  - Add `test_qualifire_config_rejects_legacy_backend_field`
    (Step 1.4 surfaced this). Assert message contains the
    migration text and `Qualifire.from_config`.
- [ ] **5.3** `tests/test_cli.py` — three concerns:
  - **Functional smoke** — verify `qualifire run <yaml>`,
    `qualifire report <yaml>`, `qualifire backfill <yaml>
    --partition ...`, `qualifire deactivate-metric ...` all
    still work after the CLI's switch to
    `_from_parsed_config`.
  - **Single-load TOCTOU guard** (codex R2 SHOULD-FIX 4) — the
    whole point of `_from_parsed_config` is to avoid a second
    `load_config()` call. Today there's a single-read guard for
    `_cmd_run`. Extend it to:
    `test_cmd_report_loads_config_once`,
    `test_cmd_backfill_loads_config_once`,
    `test_cmd_deactivate_metric_loads_config_once`. Each spies
    on `qualifire.core.config.load_config` and asserts
    `mock.call_count == 1` for the lifetime of one CLI
    invocation.
  - **Existing fixtures** — no edits beyond Step 3 (verified:
    only `system_table_backend:` matches remain in tests/).
- [ ] **5.4** Sanity sweep: full `pytest -q`. Any test that loads a
  YAML with `backend:` should have been caught in Step 3; if
  pytest surfaces stragglers, fix them as a sub-step here.

**Exit criteria for Step 5**: full `pytest -q` is green; new
`test_api_from_config.py` covers all 15 cases above.

### Step 6 — Documentation

- [ ] **6.1** `docs/programmatic_api.md` — new section
  "Constructing from YAML" placed before the existing direct-
  construction section. Show the
  `Qualifire.from_config(path, backend=...)` pattern as
  recommended; keep direct construction as the programmatic-only
  path.
- [ ] **6.2** `docs/configuration.md` — note that
  `system_table` / `system_table_backend` / `jdbc` / `owner` /
  `bu` / `notifications` flow into the instance via `from_config`.
  Document the `backend:` field removal with the migration line
  (which mirrors Step 1.2's validator message).
- [ ] **6.3** `docs/getting_started.md:202` — update the
  configuration example to drop `backend:` and show
  `Qualifire.from_config(...)`.
- [ ] **6.4** `README.md` — replace the canonical "first ten lines"
  example.
- [ ] **6.5** Migration note in the next release notes — single
  paragraph: "`backend:` field removed from YAML schema. Drop the
  line; pass the runtime backend to `Qualifire.from_config(...)`
  or `Qualifire(backend=...)` for programmatic-only setups."

**Exit criteria for Step 6**: `grep -rn 'backend: "spark"\|backend:
spark' docs/ --exclude-dir=features` returns only the migration
note. The `--exclude-dir=features` scopes the grep to live docs;
the historical idea.md and any future feature-history files are
explicitly outside scope.

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Out-of-tree consumers carry YAMLs with `backend:` and break on upgrade | Step 1.2's `model_validator` raises with the migration text. Release note one-line edit. |
| Operators relied on construction-time `system_table` differing from YAML to do "intentional reinit" | `warn_on_reinit=False` opt-out preserves silent behavior; default True for `from_config` only flips the warning, not the reinit itself. |
| Notifier merge precedence inverts on a path that uses `_build_notifiers` directly | Step 5.1 has explicit engine-handoff tests for every entrypoint (`run_config_parsed`, `backfill`, `validate`, `validate_query`, `write_audit_publish`) that spy on the engine kwargs and assert the override notifier wins. The static-grep `test_no_raw_self_notifiers_left_in_engine_paths` is a defense-in-depth complement. Single contract: programmatic always wins everywhere. |
| `from_config` becomes another way to do the same thing → API surface bloat | Direct construction is the no-YAML path; `from_config` is for YAML-driven setups. Two distinct purposes. |
| Manual notebooks (per current directive) can't be touched by Claude | Out of scope per shipped-feature pattern; flagged for operator action in a follow-up. |
| Adding `model_validator(mode="before")` could mask other useful Pydantic validation messages | Validator only fires on the specific `backend` key; other validation (missing `system_table`, type mismatches) flows through normally. |
| Single notifier-precedence contract is a behavior change for paths that relied on YAML clobbering programmatic instances | Documented as breaking change in `docs/CHANGELOG.md`. Operators relying on the legacy semantics (rare — testing scenarios usually surfaced the bug) update their YAML or test scaffolds. The single contract eliminates a class of footguns where a captured/test notifier silently disappears mid-run. |

## Test Plan Summary

- New: `tests/test_api_from_config.py` (15 tests above).
- Updated: `tests/test_config.py` (legacy field rejection),
  `tests/test_cli.py` (CLI factory switch).
- Sanity: full `pytest -q` green; YAML migration completeness
  verified by `grep -rn '^backend:' docs/ examples/ tests/`
  returning empty.

## Implementation Order Notes

Sequential: Steps 1 → 2 → 3 → 4 → 5 → 6. Step 1 (schema removal +
migration validator) must land before any Step-3 YAML edits, or
else `load_config` errors during the migration. Steps 5 (tests)
and 6 (docs) can overlap with each other but both depend on
Steps 1–4 landing first. The whole sequence is one PR.

## Acceptance Checklist

- [ ] `Qualifire.from_config(path, backend=...)` works end-to-end
      against `tests/manual/configs/end_to_end_sources.yaml` and
      `end_to_end_wap.yaml` (after `backend:` line removal).
- [ ] `QualifireConfig` Pydantic model rejects `backend:` with a
      clear migration message via the `model_validator(mode="before")`.
- [ ] CLI `qualifire run`, `qualifire report`, `qualifire backfill`,
      `qualifire deactivate-metric` all run through the factory
      internally (via `_from_parsed_config` to preserve TOCTOU-clean
      single-load).
- [ ] Programmatic kwargs override YAML on every documented field.
- [ ] Programmatic `notifiers={}` win over YAML at construction;
      tested.
- [ ] `warn_on_reinit=True` default for `from_config` warns on
      multi-YAML disagreement; `False` silent. Direct
      `Qualifire(...)` callers default to silent (existing
      behavior preserved).
- [ ] `secret_resolver` resolution stays lazy.
- [ ] All YAML examples in the repo lose the `backend:` line.
- [ ] `examples/backfill_quickstart.py` keeps its programmatic-only
      construction (4.4a accepted).
- [ ] `docs/programmatic_api.md`, `docs/configuration.md`,
      `docs/getting_started.md`, README, release notes
      updated. (`docs/cli.md` does not exist in this repo and is
      not added by this PR.)
- [ ] Full `pytest -q` green.
- [ ] Manual notebooks (`tests/manual/notebook_*.ipynb`) flagged
      for operator update; not modified by this PR.
