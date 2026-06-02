---
shipped: 2026-05-04
---

# Shipped: Spark-Primary Industry Packs + Examples README Cleanup

## Summary

Flips five previously-pandas-only E2E scenarios (`shape_pass`,
`shape_fail`, `query_join`, `pattern_pass`, `pattern_fail`)
across the three industry packs (retail, life_sciences,
financial_services) from `skip_pandas_only` to `required` so
both pandas and Spark backends are exercised. Unlocks the 15
hardcoded pandas call-sites in the industry E2E modules and
refreshes README + config.yaml comments that referenced the
pandas-only coverage claim.

## Key Changes

- **Manifest flip** — `tests/test_e2e_industries/_skip_manifest.py`:
  five `_ROWS_PER_INDUSTRY` entries changed from
  `skip_pandas_only` to `required`.
- **Call-site unlock** — 15 hardcoded
  `_make_backend("pandas", ...)` calls across retail /
  life_sciences / financial_services flipped to
  `_make_backend(backend_type, ...)`. `_skip_spark()` shim calls
  removed.
- **Permanent severity instrumentation** — six `[SEVERITY-GATE]`
  stdout prints added at accepted-range assertion sites for
  cross-run severity-uniformity verification.
- **README + config.yaml cleanup** — strikes the pandas-only
  coverage claim across the three industry pack READMEs.

## Files Changed

13 files; +1,214 / −108 lines.

## Plan PR

[#5](https://github.com/amitranjan-oracle/qualifire/pull/5) —
`feat(spark-primary): promote Spark to primary on five Pattern/Shape/Query-JOIN scenarios + README/YAML cleanup`

## Review Cycles

Plan: 2 codex rounds (review notes on local diff-scope gate +
.gitignore whitelist). Implementation: 2 codex impl-review rounds
addressing README leak fix + prewarm comment + lazy-import
phrasing.
