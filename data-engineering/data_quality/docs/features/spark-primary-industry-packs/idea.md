---
id: spark-primary-industry-packs
name: Spark-Primary Industry Packs + Examples README Cleanup
type: Enhancement
priority: P2
effort: Medium
impact: Medium
created: 2026-04-21
---

# Spark-Primary Industry Packs + Examples README Cleanup

## Problem Statement

The three industry demo packs (Retail, Life Sciences, Financial Services)
that shipped with `industry-demo-suites` exercise all 13 scenarios on the
**pandas** backend, but only a subset on the **Spark** backend. Shape,
Pattern, and Query JOIN scenarios are explicitly skipped on Spark via
`tests/test_e2e_industries/_skip_manifest.py` (`skip_pandas_only`).

That decision was made for pragmatic reasons during the initial pack work
— not because anything is broken in the library:

- `qualifire/validation/isolation_forest.py` (Shape) auto-converts Spark
  DataFrames to pandas via `.toPandas()` when needed.
- `qualifire/validation/pattern_check.py` (Pattern) does the same.
- `qualifire/backends/spark_backend.py` already implements `execute_sql`
  for query datasets and `sample_records` for the samplers.

So the Spark skips are a **fixture/scope convention**, not a runtime
constraint. We want every check to be exercised against Spark in CI,
treating pandas as the optional/secondary backend for these demo packs.
For small-volume demo data (≤10k rows per dataset, per the existing
budgets), it is acceptable for Spark DataFrames to be converted to
pandas internally for sklearn-backed validators.

A second, related issue surfaced in the same conversation: the
**user-facing README files under `examples/industries/`** leak
test-harness facts that have nothing to do with the YAML configs they
document. Specifically:

- A trailer sentence on each README — *"Spark skips for shape,
  query_join, and pattern are intentional and sanctioned by
  `tests/test_e2e_industries/_skip_manifest.py`"* — references a pytest
  manifest that lives in our test tree, not in the example.
- A "Backends" column on each scenario table marks rows as `pandas only`
  on the same basis. The YAML configs themselves are backend-agnostic at
  runtime — whichever backend `qualifire run` is invoked with is what
  executes them. Once the Spark coverage gap above is closed, the column
  values become outright wrong; even before then, they describe our
  internal CI choice rather than a property of the YAML.

These two scopes are bundled into one feature because closing the Spark
coverage gap is what makes the README cleanup truthful — fixing the
README without flipping the manifest would just trade one inaccuracy
for another.

## Affected Areas

- tests/test_e2e_industries (manifest, fixtures, parametrization)
- tests/_e2e_support (per-industry generators — Spark temp-view plumbing)
- examples/industries (README and config.yaml docs)
- CI runtime budgets (per-pack and total — JVM + per-scenario Spark cost)
