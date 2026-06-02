---
shipped: 2026-05-09
---

# Shipped: AND-combine Collector Filter with Dataset Filter

## Summary

**BREAKING CHANGE.** Pre-2026-05-09, when both a dataset-level
`filter:` and a per-collector `filter:` were set on the same
collection, the collector's filter silently *replaced* the
dataset's filter — operators got narrower scope than their
config implied. v1 AND-combines them: `(dataset.filter) AND
(collector.filter)`, applied uniformly across every filtered
collector type (aggregation, metrics, profiling, custom_query).

## Key Changes

- AND-combine logic in the collector filter resolver; consistent
  across all filtered collector types.
- Per-collector filter expressions parenthesized to preserve
  operator-supplied semantics under combination.
- Runtime cache bypass for filtered collectors (placeholder
  pending the broader skip-recollection redesign).
- Documentation refresh: `docs/collectors/README.md` plus
  per-collector pages.

## Files Changed

17 files; +1,739 / −31 lines. New test file
`tests/test_collection/test_filters.py` (1,128 cases) covering
every filter combination shape across collectors.

## Plan PR

[#11](https://github.com/amitranjan-oracle/qualifire/pull/11) —
`feat(and-combine-collector-filter): plan + implementation`

## Migration

CHANGELOG entry under "Breaking" lists every collector that
ships the new AND-combine semantics. Operators with both
filters set need to verify the combined scope matches their
intent before upgrading.
