---
shipped: 2026-04-21
---

# Shipped: Industry Demo and E2E Test Suites

## Summary

Three self-contained industry demo packs (Retail, Life Sciences, Financial
Services) shipped as both user-facing demos under `examples/industries/` and
as the CI regression gate under `tests/test_e2e_industries/`. Each pack
exercises all six validation types — SLO, Threshold, Drift, Trend, Shape,
Pattern — plus Query-JOIN via `DatasetConfig(query=...)`, with legitimate
pass/fail scenarios on seeded synthetic data. One `report.html` per pack
is emitted from a pack-scoped autouse finalizer.

## Key Changes

- **Three industry packs** (Retail SEED=2026, Life Sciences SEED=2027,
  Financial Services SEED=2028) — 13 scenarios each × 2 backends = 26
  parametrizations per pack, with Shape/Pattern/Query-JOIN on Spark
  explicitly skipped via `_skip_manifest.py` (pandas-only sanctioned).
- **Shared E2E harness** extracted into `tests/_e2e_support/` (backends,
  reporting, per-industry seeded generators) — move-only refactor of the
  original `tests/test_e2e.py` helpers.
- **Single source of truth for coverage** — `_skip_manifest.py` is enforced
  in two surfaces: in-process `pytest_collection_modifyitems` hook
  (collection-time), and a `--verify-skips` CLI that reads JUnit XML
  post-run (reason-string enforcement + required-rows-executed guard).
- **Pack-scoped report finalizer** — one `report.html` per pack via
  autouse package-scope fixture reading a shared `SQLiteStorage`. Emits
  even on mid-pack failure (pytest LIFO teardown).
- **CI contract in `.github/workflows/tests.yml`** — installs pyspark
  explicitly, exports `QUALIFIRE_ARTIFACTS_DIR`, single pytest invocation
  producing JUnit XML, post-test assertion + duration gate (180s/pack,
  600s total) with matched==0 guard and classname fallback (no vacuous
  passes). 5 MB per-industry artifact budget via `du -sb`.
- **Spark JVM prewarm** — session-scoped autouse fixture amortizes JVM
  startup across packs. Hardened (post-review) with two guards: skip
  when no `[spark]` tests are collected, and broad-exception wrap around
  `getOrCreate()` so a broken-but-installed pyspark emits a
  `RuntimeWarning` rather than aborting the session.
- **Pattern leakage control** — per-industry `exclude_columns` lists
  locked and required to match between YAML and pytest. FAIL-row
  assertions require `top_contributing_features is not None` to catch
  SHAP soft-fails.
- **User-facing docs** — `examples/industries/<industry>/README.md` for
  each pack with scenario coverage tables and config walkthrough;
  `examples/industries/<industry>/config.yaml` (backend-agnostic at
  runtime) mirroring the pytest scenarios for inventory.

## Files Changed

- `.github/workflows/tests.yml` (new) — single pytest gate in CI
- `docs/features/industry-demo-suites/plan.md` — 5 rounds of Codex review
  iterations
- `examples/industries/retail/{README.md,config.yaml}` (new)
- `examples/industries/life_sciences/{README.md,config.yaml}` (new)
- `examples/industries/financial_services/{README.md,config.yaml}` (new)
- `tests/_e2e_support/` — new shared harness (backends, reporting,
  per-industry generators)
- `tests/test_e2e_industries/` — new pack tree (retail, life_sciences,
  financial_services) + `_skip_manifest.py` + `conftest.py`
- `tests/test_e2e.py` — slimmed to reuse extracted harness

Merge commit: `f267a42` (PR #4).

## Testing

- Full `pytest tests/` run: 33 original E2E tests + 3 × 26 industry
  parametrizations, with manifest-enforced pandas-only skip reasons.
- Per-industry duration budget 180s, total 600s, enforced from JUnit XML.
- Post-review regression reproducer:
  `pytest tests/test_e2e_industries/ -q -k 'pandas or config_yaml_inventory_matches_pytest_scenarios'`
  → 42 passed, 39 deselected, 3.84s (confirms Spark-prewarm hardening
  does not break pandas-only runs).
- Codex review: 5 implementation rounds + 1 verification pass; final
  reviewer verdict PASS on both plan and implementation.

## Notes

- **Spark coverage gap**: Shape, Pattern, and Query-JOIN scenarios
  currently skip the Spark backend via `_skip_manifest.py` as a scope
  convention (validators already auto-convert Spark DataFrames to
  pandas internally via `.toPandas()`, so the skip is not a library
  constraint). Captured as a follow-up feature:
  [`spark-primary-industry-packs`](../spark-primary-industry-packs/idea.md),
  which also covers cleanup of the test-harness leakage in
  `examples/industries/*/README.md`.
- **YAML ↔ pytest parity is inventory-only, not semantic**: demo YAMLs
  may diverge from pytest on thresholds/hyperparameters for readability.
  `config_yaml_inventory_matches_pytest_scenarios` enforces only the
  scenario-name inventory.
- **Generator lifecycle**: one call per pack run via pack-scoped fixture;
  tests that mutate (e.g., Shape corrupted-sample) take deep copies —
  re-invoking the seeded generator would break reproducibility across
  test orderings.
