---
started: 2026-05-11
---

# Implementation Plan: Zero-arg `qf.run()` reusing the construction-time config

## Overview

Cache the parsed `QualifireConfig` on the `Qualifire` instance via
**two public construction paths** — `from_config(path)` (YAML on
disk → parsed by `load_config`) and `from_parsed_config(config)`
(programmatic `QualifireConfig` object, or a pre-parsed config
the caller already holds). Both populate the same cache. Expose a
public `qf.run(...)` that delegates to
`run_config_parsed(self._cached_config, ...)` when the cache is
populated and raises a clear `QualifireConfigError` otherwise.
Surface the cache read-only via a `qf.config` property. All other
public API stays exactly as it is — `run_config(path)` and
`run_config_parsed(config)` remain the documented entry points for
the "different config each call" case.

**Parity contract:** `qf.run()` must work identically whether
the instance was built via `Qualifire.from_config(path, ...)` or
`Qualifire.from_parsed_config(parsed, ...)`. This means
**promoting the current `_from_parsed_config` (single-underscore,
"stable enough to test; not part of the documented public API
surface") to public `from_parsed_config`**. The CLI's
`_from_parsed_config` callsite is rewritten in the same change.

Effort: Small. Roughly **+50 LoC in `api.py`**, **+5 tests in
`tests/test_api_from_config.py`**, doc updates in three files,
plus one CLI callsite rename.

## Answers to the open questions from idea.md

| # | Question | Decision |
|---|----------|----------|
| 1 | Re-read vs frozen-cache semantics | **Frozen at construction.** The cache holds the `QualifireConfig` parsed in `from_config(...)`. No opt-in reload flag this feature. Users who want fresh YAML call `from_config(path)` again or `run_config(path)`. |
| 2 | Backfill / health-report parity | **Defer.** `qf.run()` only. A future "zero-arg-backfill-and-health-report" feature can extend the pattern once `qf.run()` has bedded down. Listed as a non-goal in `idea.md`. |
| 3 | Error class for direct-construction case | **Reuse `QualifireConfigError`** (defined at `qualifire/core/exceptions.py:32`). Adding a new class for one call site isn't worth the exception-handling churn for callers. |
| 4 | Cache visibility | **Read-only property** `qf.config: QualifireConfig \| None`. No setter. Users get to inspect / log / pass-to-`run_config_parsed` if they want. The underlying object is mutable (Pydantic model) by design — users *can* mutate it and re-call `qf.run()`; this is the same contract as if they held a parsed config externally. |
| 5 | CLI integration | **CLI switches from `_from_parsed_config` to the new public `from_parsed_config`** (single rename — no behavior change). It continues to call `run_config_parsed(parsed)` rather than `qf.run()` for the TOCTOU-guard reason: it already holds the parsed object after preflight, and `qf.run()` would force a redundant cache lookup. |

## Scope gates / hard-stop conditions

The implementation MUST NOT do any of the following. If a reviewer or
agent suggests one, push back with the cited gate:

1. **HARD STOP — no signature or semantics change to `run_config(path)`
   or `run_config_parsed(config)`.** Both stay byte-identical to the
   current implementation. They are independently testable from
   `qf.run()`.
2. **HARD STOP — no new fields on `QualifireConfig`.** The cache lives
   on `Qualifire`, not on the config.
3. **HARD STOP — no zero-arg variants for `backfill`, `validate`,
   `health_report`, `deactivate_metric`.** Scope-creep guard. Future
   feature.
4. **HARD STOP — no deprecation, warning, or rename of
   `run_config(path)`.** It is the documented path for the "different
   YAML each call" case and stays first-class.
5. **HARD STOP — no per-call YAML re-read** inside `qf.run()`. The
   frozen-cache contract is non-negotiable for this feature.
6. **HARD STOP — no behavior change in the CLI.** The CLI's call
   to `_from_parsed_config` is renamed to `from_parsed_config` (the
   new public name) and nothing else. No rewrite to route through
   `qf.run()` — the CLI's TOCTOU-guarded preflight pattern stays.
7. **HARD STOP — no copy at the cache-assignment or
   property-exposure boundary.** Inside Step 2's
   `from_parsed_config`, do **not** wrap, re-validate, or
   copy the parsed config before assignment to
   `instance._cached_config`. Inside Step 3's `qf.config`
   property, return `self._cached_config` directly — no
   `model_copy()`, no `copy.deepcopy(...)`, no
   `QualifireConfig(**dump)`. The contract is *"cached by
   reference, frozen-at-construction-time"*. The parsed object
   handed to `from_parsed_config(config, ...)` is the **same
   Python object** returned by `qf.config`
   (`qf.config is config`). Copies obscure the
   `qf.config.datasets[0].partition_ts = ...; qf.run()` workflow
   that some users may legitimately want; identity preservation
   is what makes that workflow actually work.

   **Scope note:** this hard-stop does NOT touch the existing
   copy that `run_config_parsed` performs internally via
   `_resolve_secrets_or_raise` (which rebinds to a
   secret-resolved config before engine execution to avoid
   mutating the caller's config — see `qualifire/api.py:436`).
   That behavior is correct and stays. The hard-stop is scoped
   to the cache-write and property-read sites only.

## Implementation Steps

Each step has an explicit exit criterion verifiable from `git diff`.
Do them in order — Step 4 depends on Step 1 / Step 2 / Step 3.

### Step 1 — Initialize the cache slot in `Qualifire.__init__`

- File: `qualifire/api.py`
- Add `self._cached_config: QualifireConfig | None = None` in
  `__init__` (around line 157, next to `self._notifiers = ...`).
- Import `QualifireConfig` at module top if not already imported (it
  is — `from qualifire.core.config import QualifireConfig, load_config`).
- **Exit criterion:** `git diff qualifire/api.py` shows one new line
  in `__init__`:
  ```python
  self._cached_config: QualifireConfig | None = None
  ```

### Step 2 — Promote `_from_parsed_config` to public `from_parsed_config` and populate the cache

- File: `qualifire/api.py`
- Rename `_from_parsed_config` (currently at line 264) to
  `from_parsed_config` — drop the leading underscore. Signature
  unchanged. Same `@classmethod`, same kwargs, same
  `accepted_overrides` set.
- Inside the renamed method, capture the constructed instance and
  set `instance._cached_config = config` before returning. The
  current `return cls(...)` becomes:
  ```python
  instance = cls(...)
  instance._cached_config = config
  return instance
  ```
- Update the docstring to:
  1. Drop the "Single leading underscore signals 'stable enough
     to test; not part of the documented public API surface.'"
     paragraph — it's public now.
  2. State the parity contract: this is the parsed-object twin
     of `from_config(path)`; both populate the cache and both
     enable `qf.run()`.
  3. State that the parsed config is cached on the instance for
     `qf.run()`.
- Update the caller inside `from_config` (line 254) to point at
  the new public name:
  ```python
  return cls.from_parsed_config(
      config,
      backend=backend,
      ...
  )
  ```
- **Exit criterion:** `git diff qualifire/api.py` shows:
  - Method renamed (no underscore prefix).
  - `_cached_config` assignment line.
  - Docstring updated per the three points above.
  - `from_config` body calls `from_parsed_config` (no underscore).
  - `cls(...)` argument list is byte-identical to today.

### Step 2b — Propagate the rename to all 20 occurrences

The rename in Step 2 must reach every existing reference to
`_from_parsed_config` since this is pre-release (no
backwards-compat alias). Authoritative survey (`grep -rn
"_from_parsed_config" qualifire tests`):

**Callsites (5):**
- `qualifire/cli.py:264` — `Qualifire._from_parsed_config(...)`
- `qualifire/cli.py:339` — `Qualifire._from_parsed_config(...)`
- `qualifire/cli.py:425` — `Qualifire._from_parsed_config(...)`
- `qualifire/api.py:255` — `cls._from_parsed_config(...)` inside
  `from_config` (already covered by Step 2's edit list, listed
  here for completeness).
- `tests/test_api_from_config.py:747` — parity test calls
  `Qualifire._from_parsed_config(config, backend=None)`.

**Real method definition (1):**
- `qualifire/api.py:265` — `def _from_parsed_config(...)`.
  Renamed by Step 2 itself; listed here for accounting.

**Fake `Qualifire` classmethod definitions in test_cli.py (4):**
These are NOT `mock.patch.object` target strings — they are
locally-defined subclass methods that override the real
classmethod via class-attribute dispatch. They must be renamed
so the CLI's call to the new `from_parsed_config` still hits
them.
- `tests/test_cli.py:691` — `def _from_parsed_config(cls, cfg, **kwargs):`
- `tests/test_cli.py:776` — same shape
- `tests/test_cli.py:869` — same shape
- `tests/test_cli.py:935` — same shape

**Assertion message string in test_cli.py (1):**
- `tests/test_cli.py:694` — inside an assertion message string
  literal: `"_from_parsed_config, not a path string"`. Rename
  the literal to `"from_parsed_config, not a path string"`.

**Comment / docstring textual refs (9):**
- `qualifire/api.py:249` — `:meth:`_from_parsed_config``
  cross-reference in `from_config` docstring.
- `qualifire/cli.py:258` — comment.
- `qualifire/cli.py:420` — comment.
- `tests/test_api_from_config.py:2` — module docstring.
- `tests/test_api_from_config.py:733` — section comment
  (`# 9. ``_from_parsed_config`` parity`).
- `tests/test_api_from_config.py:742` — test-method docstring.
- `tests/test_cli.py:745` — docstring.
- `tests/test_cli.py:836` — docstring.
- `tests/test_cli.py:738` if any further textual ref shows up
  in the rename — verify via grep before declaring done.

**Total: 20 occurrences across 4 files.** Update every one.
No behavior change in any callsite — just the symbol name,
the assertion message literal, and the textual references that
mention it.

**Exit criterion:**
```
grep -rn "_from_parsed_config" qualifire tests
```
returns zero matches across `qualifire/` and `tests/`. All
existing tests continue to pass (especially the 4 fake-
classmethod tests in `test_cli.py`, which only pass if the
fake's signature still matches what the CLI calls).

### Step 3 — Add a read-only `config` property

- File: `qualifire/api.py`
- Add a `@property` named `config` returning `QualifireConfig | None`
  (whatever `_cached_config` is). No setter. Short docstring:
  *"The parsed YAML config captured at construction time, or
  ``None`` for direct ``Qualifire(...)`` construction. Mutate via
  the returned object and re-call ``qf.run()`` to re-run with
  the modified config."*
- Place the property near `register_notifier` (line 319) for
  proximity to other public surface.
- **Exit criterion:** `git diff` shows the property block. `from
  qualifire import Qualifire; Qualifire.config` resolves to a
  property object in a python repl smoke test (covered by
  the test step below).

### Step 4 — Implement `qf.run()`

- File: `qualifire/api.py`
- Signature mirrors the kwargs of `run_config_parsed`:
  ```python
  def run(
      self,
      *,
      context: dict[str, str] | None = None,
      skip_recollection: bool = False,
      skip_renotification: bool = False,
      skip_revalidation: bool = False,
  ) -> QualifireResult:
  ```
- Body:
  ```python
  if self._cached_config is None:
      raise QualifireConfigError(
          "Qualifire was constructed without a YAML config. "
          "Call Qualifire.from_config(path, backend=...) to load "
          "one, or call qf.run_config(path) to run an ad-hoc config."
      )
  return self.run_config_parsed(
      self._cached_config,
      context=context,
      skip_recollection=skip_recollection,
      skip_renotification=skip_renotification,
      skip_revalidation=skip_revalidation,
  )
  ```
- Docstring: explain the frozen-cache contract, the kwargs match
  `run_config_parsed`, link to `run_config(path)` for the
  different-YAML case, call out the `QualifireConfigError`
  raise contract, and explicitly note that `qf.run()` does
  **not** accept a `notifiers=` kwarg — runtime notifiers come
  from construction (`from_config(notifiers={...})` /
  `from_parsed_config(notifiers={...})`) or post-construction
  (`qf.register_notifier(...)`).
- Place the method **immediately above** `run_config` (line 528).
  Note this is the *relative* ordering of the public config-run
  methods — `register_notifier` (line ~319) and the
  `_effective_notifiers` / `_swap_storage_if_needed` helpers
  (lines ~331–~526) sit between `from_parsed_config` and
  `run_config` in the current source. After this insert, the
  public config-run methods read top-to-bottom as
  `from_config` → `from_parsed_config` → … helpers … →
  `run` → `run_config` → `run_config_parsed`, which is the
  intended discoverability gradient.
- `QualifireConfigError` is already imported at `api.py:42`
  (verified) — no new import needed.
- **Exit criterion:** `git diff` shows the new method.
  `python -c "from qualifire import Qualifire; help(Qualifire.run)"`
  prints the docstring. Tests in Step 8 pass.

### Step 5 — Update `docs/getting_started.md`

- Replace the canonical YAML example so it ends with `qf.run()`
  instead of `qf.run_config(path)`. Keep the path argument visible
  in a "Different YAML each call" follow-on snippet.
- **Exit criterion:** `git diff docs/getting_started.md` shows the
  edit. `grep -n "from_config\|run_config\|qf.run()"
  docs/getting_started.md` shows the new two-line happy path.

### Step 6 — Update `docs/configuration.md`

- Add a short subsection (≤ 25 lines) titled something like
  *"Running with the construction-time YAML — `qf.run()`"* under
  the existing top-level structure (find an appropriate place
  near other API-mode coverage).
- State explicitly:
  - `qf.run()` reuses the parsed config from `from_config(...)`
    or `from_parsed_config(...)`.
  - `Qualifire.from_parsed_config(config, ...)` is the
    parsed-object twin of `from_config(path, ...)` — both
    populate the same cache; both enable `qf.run()`. Use it
    when you already hold a `QualifireConfig` (built
    programmatically, or loaded once with `load_config(path)`
    and reused).
  - The cache is **frozen at construction**: editing the YAML on
    disk after `from_config(...)` does not affect `qf.run()`.
    Call `from_config(path)` again for a fresh read.
  - `qf.config` exposes the cached config read-only.
  - Direct `Qualifire(...)` construction has no cache;
    `qf.run()` raises `QualifireConfigError` pointing at
    `from_config` or `run_config(path)`.
  - `qf.run_config(path)` does **not** touch the cache —
    `qf.run()` always uses the construction-time config
    regardless of intervening `run_config` calls.
- **Exit criterion:** `git diff docs/configuration.md` shows the
  new subsection. The subsection includes all six bullet facts
  above.

### Step 7 — Verify whether README needs an update

- `grep -n "from_config\|run_config" /Users/amitranjan/IdeaProjects/qualifire/README.md`
- If a YAML example exists in the README, update it to use
  `qf.run()`. If not, no edit is needed and the plan exit
  criterion is the grep output as evidence.
- **Exit criterion:** `git diff README.md` shows the edit, **or**
  the implementation step is checked off with the grep output
  captured in the implementation notes confirming no YAML
  example existed.

### Step 8 — Add tests to `tests/test_api_from_config.py`

Add exactly five tests. Keep each ≤ 30 LoC. Use the existing
fixtures / helpers in the file.

1. **`test_run_zero_arg_roundtrip_matches_run_config`** —
   Construct via `Qualifire.from_config(tmp_yaml, backend=...)`,
   call `qf.run()`. Construct a fresh `Qualifire(...)` with the
   same instance-level fields and call `qf2.run_config(tmp_yaml)`.
   Assert both `QualifireResult` objects have the same dataset
   IDs, same per-dataset severities, and same row counts.
2. **`test_run_passes_through_skip_revalidation`** — Call
   `qf.run(skip_revalidation=True)` against a YAML that would
   otherwise emit at least one validation row; assert the
   resulting result reflects the skip (matches the behavior of
   `qf.run_config(path, skip_revalidation=True)` byte-for-byte).
3. **`test_run_without_cached_config_raises_qualifire_config_error`** —
   Construct via `Qualifire(backend=..., system_table=...,
   system_table_backend="sqlite")` (no YAML). Call `qf.run()`.
   Assert `pytest.raises(QualifireConfigError)` and that the
   message includes both `"Qualifire.from_config"` and
   `"qf.run_config"` as substrings (qualified tokens — prevents
   `run_config_parsed` from accidentally satisfying the bare
   `"run_config"` match if a future docstring rewrite drops the
   `qf.` prefix). The user is pointed at both paths.
4. **`test_run_uses_frozen_cache_after_yaml_edit`** — Create a
   YAML using the existing `_write_yaml` helper / `tmp_path`
   fixture (see `tests/test_api_from_config.py:34-40`). Build
   `qf = Qualifire.from_config(path, ...)`. Then **rewrite the
   same path** (use `_write_yaml(path, different_payload)` or a
   plain `path.write_text(...)`) with a modified config — e.g.,
   remove a dataset. Call `qf.run()` and assert the result
   still contains the **original** dataset (proves the cache is
   frozen-at-construction; the on-disk edit was not re-read).
   No existing test in this file follows the write-then-edit
   pattern — this is the first.
5. **`test_run_from_parsed_config_object_parity`** — The parity
   ask. Build a `QualifireConfig` object **without touching the
   filesystem** (either via `QualifireConfig(...)` direct
   construction or by calling `load_config(path)` once and
   reusing the parsed object). Construct via
   `Qualifire.from_parsed_config(config, backend=...)`, call
   `qf.run()`, and assert the result is shape-equivalent to
   `Qualifire.from_config(equivalent_path, ...).run()`. Also
   verify `qf.config is config` (same object reference — the
   parity contract preserves the parsed object).

- **Exit criterion:** `pytest tests/test_api_from_config.py -k "run or parsed_config"`
  passes with the five new tests visible in the collection output.
  Full `pytest tests/` passes (no regressions).

### Step 9 — `docs/CHANGELOG.md` entry

- Add an Enhancement entry under the next un-shipped version
  block (or the appropriate location — match the convention of
  recently shipped features like `deck-content-port-to-repo-docs`).
- One line referencing the feature ID and the API addition.
- **Exit criterion:** `git diff docs/CHANGELOG.md` shows the new
  entry naming `Qualifire.run()`, `Qualifire.config`, and the
  feature ID `zero-arg-run-from-config`.

## Technical Decisions

| Decision | Rationale |
|---|---|
| Cache point in `from_parsed_config`, not in `from_config` | Both public entry points (`from_config(path)` and `from_parsed_config(parsed)`) flow through here. Single cache-assignment site avoids drift. |
| Default `_cached_config = None` in `__init__` | Direct `Qualifire(...)` construction must produce a `qf` whose `qf.run()` raises a clear error. Initializing to `None` is the cheapest contract. |
| Reuse `QualifireConfigError` | Already covers `from_config` validation failures; the "no config cached" case fits naturally. No new exception class to plumb through users' `except` blocks. |
| Read-only property over a public `qf._cached_config` attribute | Property names the cache explicitly, allows future internal refactors without changing the public surface. |
| No copy of cached config (identity preserved) | `qf.config is config` for the construction-time object. Preserves the "edit and re-run" workflow; aligns with Pydantic's mutable-model default. |
| `qf.run()` placed *above* `run_config` in source order | Reading top-to-bottom, `from_config → from_parsed_config → run → run_config → run_config_parsed` is the right discoverability gradient: easiest first, lowest-level last. |
| CLI behavior unchanged; symbol name updated | Step 2b renames the CLI's 3 callsites from `_from_parsed_config` to `from_parsed_config` (and updates 2 comments). No behavior change — the CLI's preflight + parsed-object pattern stays. Hard-stop #6. |

## Testing Strategy

- **Unit tests:** five new tests in `tests/test_api_from_config.py`
  covering round-trip, skip-flag pass-through, direct-construction
  error path, freeze contract, and parsed-object parity (above).
- **Existing tests:** full `pytest tests/` run must pass with zero
  regressions. The change is purely additive in `api.py`; existing
  call sites of `run_config` / `run_config_parsed` continue to work
  identically.
- **Manual smoke test (optional, recommended):** in a python REPL,
  load any existing test fixture YAML via `Qualifire.from_config`,
  call `qf.run()`, confirm output. Documented in implementation
  notes if performed.

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Users confused about which YAML "wins" when they mix `from_config(p1)` and `run_config(p2)` | Documented contract: `run_config(p2)` is independent of the cache; `qf.run()` always uses the cache. Doc subsection in Step 6 states this. |
| Users mutate `qf.config` in place and are surprised by the next `qf.run()` reflecting it | This is the documented contract (Step 3 docstring + Step 6 doc). Users who want immutability can wrap with `QualifireConfig.model_copy()` themselves. |
| Reviewer (Codex) asks "why not deep-copy the cached config defensively?" | Hard-stop #7 + the technical-decisions rationale above. Defensive copies obscure the legitimate "edit-then-rerun" workflow and are not how the rest of the API behaves with Pydantic models. |
| Reviewer asks "should the CLI use `qf.run()` for consistency?" | Hard-stop #6 + Q5 rationale: the CLI holds a parsed config after preflight; routing through the cache adds no value and slightly obscures the TOCTOU rationale. |
| Test #4 (freeze contract) is flaky if YAML on-disk edits race the test runner | Tests use synchronous file writes and call `qf.run()` only after the write completes. Standard pytest fixture pattern (already in use elsewhere in `test_api_from_config.py`). |
| Pydantic `QualifireConfig` is large and the import in `api.py` already happens — no extra cost | Verified by grep: `from qualifire.core.config import QualifireConfig, load_config` is already there. |
| `qf.run_config(other_path)` between construction and `qf.run()` — does the cache survive? | **Yes.** `run_config` / `run_config_parsed` do not touch `self._cached_config`. `qf.run()` always uses the construction-time config. Documented in the Step 6 doc subsection; not tested as a primary contract surface (treated as advanced-use behavior). |
| Storage-swap residue when `run_config_parsed(other)` flipped `self._storage` before `qf.run()` | `qf.run()` will run the cached config through `_swap_storage_if_needed` which may swap storage back to the cached config's `system_table`. The user sees a `QualifireReinitWarning` they may not expect. Documented in the Step 6 doc subsection as a known cost of mixing `qf.run()` with `qf.run_config(other)` in the same session. |

## Out-of-scope (deferred to future features)

- Zero-arg `qf.backfill()` / `qf.validate()` / `qf.health_report()` /
  `qf.deactivate_metric()` — pattern can extend once `qf.run()` ships.
- An opt-in `from_config(path, cache_config="reload")` flag for the
  re-read-on-each-call mental model.
- A `qf.reload_config()` helper that re-parses the original path —
  same use case as calling `from_config(path)` again.
