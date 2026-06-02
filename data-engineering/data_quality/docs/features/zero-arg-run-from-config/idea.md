---
id: zero-arg-run-from-config
name: Zero-arg qf.run() entry point reusing the construction-time YAML
type: Enhancement
priority: P2
effort: Small
impact: Medium
created: 2026-05-11
---

# Zero-arg `qf.run()` entry point reusing the construction-time YAML

## Problem Statement

The canonical YAML-driven happy path today reads the same YAML
twice:

```python
qf = Qualifire.from_config("qualifire.yml", backend=spark)
qf.run_config("qualifire.yml")            # YAML re-loaded
```

`Qualifire.from_config(...)` lifts only **instance-level** fields
(`owner`, `bu`, `system_table`, `system_table_backend`, `jdbc`)
onto `self`. The **per-run** fields (`datasets`, `notifications`,
`engine_notify`, `partition_ts`, `partition_step`,
`dataset_parallelism`, `context`) stay attached to the parsed
config and are not cached on the instance. So `run_config()` /
`run_config_parsed()` are the only public ways to actually run
the YAML, and each call re-passes the path or the parsed object.

The CLI avoids the double-read by reaching into the
underscore-marked `_from_parsed_config` + `run_config_parsed`
pair (single load in preflight, parsed object handed through).
That works because the CLI is internal — it's not advertised as
the public ergonomic path for notebook / library users.

The cost today is **cognitive friction**, not correctness:

- The double file read confuses users who edit YAML between
  construction and run (today the second read wins; one
  reasonable mental model expects construction-time freeze).
- The path is passed twice for no reason a caller can articulate
  from the type signature.
- The most-discoverable surface (`Qualifire.from_config(...)`)
  doesn't actually let you *run* anything without a second call
  that re-takes the same input.

## Why It Matters

- **Notebook / library ergonomics.** The YAML-driven flow is the
  primary on-ramp for new users (see
  `docs/getting_started.md`); shaving the canonical example to
  two lines without losing flexibility is a real UX win for the
  evaluator persona.
- **Honest API surface.** Today the CLI's "read once" pattern is
  better than what we offer notebook users. Closing the gap
  brings the public API up to the internal one.
- **No-op for power users.** `run_config(path)` and
  `run_config_parsed(config)` stay exactly as they are — the new
  entry point is purely additive.

## Who Benefits

- **First-time qualifire users** following
  `docs/getting_started.md`, who currently copy a two-step
  invocation that looks redundant.
- **Notebook authors on AIDP Workbench** running the same
  config repeatedly across cells; today they either repeat the
  path or reach for the underscore-marked internal API.
- **AIDP account engineers** demoing qualifire — one line less
  on the slide.

## Scope

### A — Cache the parsed config on the instance during `from_config`

- `Qualifire.from_config(path, backend=...)` parses the YAML
  once (already does), then attaches the parsed
  `QualifireConfig` to the instance as a new private attribute.
- Direct `Qualifire(backend=..., system_table=..., ...)`
  construction does **not** populate the cache (no YAML was
  loaded). This is the contract distinguishing "I have a config"
  from "I built this programmatically".

### B — Add public `qf.run(...)` entry point

- Zero positional args. Accepts the same keyword args as
  `run_config_parsed`:
  `context=`, `skip_recollection=`, `skip_renotification=`,
  `skip_revalidation=`.
- When the cache is present, runs against the cached config and
  returns a `QualifireResult` identical to what
  `run_config_parsed(self._cached_config, **kwargs)` would have
  returned.
- When the cache is absent (direct construction), raises a
  clear `QualifireConfigError`:
  *"No YAML was loaded at construction. Use `Qualifire.from_config(path, ...)` or call `qf.run_config(path)` directly."*

### C — Documentation update

- `docs/getting_started.md` — canonical YAML example becomes
  the two-line form. Keep the `run_config(path)` example as a
  follow-on for "use a different YAML each call".
- `docs/configuration.md` — short subsection introducing
  `qf.run()` and stating the cache semantics (frozen at
  construction; re-load by calling `from_config(path)` again).
- README — if the README shows a YAML example, update it too.
- `docs/CHANGELOG.md` — Enhancement entry.

### D — Tests

- Round-trip: `Qualifire.from_config(p, ...).run()` produces the
  same `QualifireResult` shape as
  `Qualifire(...).run_config(p)` for the same YAML.
- `qf.run(skip_revalidation=True)` matches the existing
  `run_config(p, skip_revalidation=True)` path.
- Direct `Qualifire(...)` followed by `qf.run()` raises
  `QualifireConfigError` with the expected message (caller is
  pointed at `from_config` / `run_config(path)`).
- Editing the YAML *after* `from_config(...)` and calling
  `qf.run()` returns results based on the **cached** copy
  (proves the freeze contract).

## Non-goals

- **No change to `run_config()` / `run_config_parsed()` /
  `validate()` / `backfill()` semantics.** Both remain the
  documented public API.
- **No new fields on `QualifireConfig`.**
- **No deprecation of `run_config(path)`.** Both paths stay
  forever — the "different YAML each call" use case is real.
- **No zero-arg `qf.backfill()` / `qf.health_report()` in this
  feature.** Plan phase decides whether to extend the pattern
  later; this feature ships `qf.run()` only to keep the change
  surface small.
- **No re-read semantics.** The cached config is frozen at
  construction. If users want a fresh read they can call
  `from_config(...)` again or use `run_config(path)`.

## Affected Areas

- `qualifire/api.py` — cache parsed config in `from_config`
  (and `_from_parsed_config`); add `run()` method; raise
  helpful error when cache absent.
- `tests/` — new round-trip + freeze-contract tests.
- `docs/getting_started.md` — canonical example update.
- `docs/configuration.md` — short `qf.run()` subsection.
- `README.md` — only if it currently shows a YAML example.
- `docs/CHANGELOG.md` — Enhancement entry.

## Open Questions for Planning

1. **Re-read vs frozen-cache semantics.** The scope above pins
   the frozen-cache contract (cache is the parsed config from
   construction). Should plan phase consider an opt-in
   `Qualifire.from_config(path, cache_config="reload")` flag for
   users who *want* the file re-read on each `qf.run()`? Or
   defer — frozen is the cleaner default and the second path
   (`run_config(path)`) already covers the re-read case.
2. **Backfill / health-report parity.** Should `qf.backfill()`
   and `qf.health_report()` get the same zero-arg treatment in
   this feature, or split into a follow-up? Scope above splits
   them out; plan phase confirms whether keeping them together
   would actually be cheaper.
3. **Direct-construction error class.** Should the "no YAML
   cached" failure raise `QualifireConfigError` (matches
   existing config-error vocabulary) or a new dedicated class
   like `QualifireNoConfigCachedError`? Plan phase decides;
   probably reuse `QualifireConfigError`.
4. **Cache visibility.** Plan phase decides whether the cached
   config is exposed read-only via a property
   (`qf.config: QualifireConfig | None`) for users who want to
   inspect or modify-then-`run_config_parsed`, or kept entirely
   private. Read-only property is the cheaper default.
5. **CLI integration.** The CLI today uses
   `_from_parsed_config + run_config_parsed`. Once `qf.run()`
   lands as the public entry point, should the CLI switch to it
   (one less underscore-API caller), or stay on the parsed-
   pair for the explicit TOCTOU-guard reason? Plan phase
   confirms.
