---
shipped: 2026-05-04
---

# Shipped: Findings Sweep

## Summary

Coordinated single-PR sweep of 5 Blocking + 9 Should-fix items
from the combined Codex+Claude review at `.tmp/findings.md`, plus
a typed identity-keys architectural refactor that sat underneath
several correctness bugs, plus dashboard auto-gen hook hardening.

## Key Changes

### P1 — Identity foundations
- `MetricKey` / `ValidationKey` types in `core/models.py`.
- `validation_base_name` field on `ValidationResult` (kw_only required).
- `dimension_value` persisted on validation rows (was hardcoded NULL).
- `val_config.name` honored for every validator (not just pattern).
- Public builders accept `name=`.

### P2 — Correctness fixes
- threshold / historical / forecast no longer collapse multi-dim rows;
  one `ValidationResult` per `(metric, dimension)`.
- drift / forecast `read_metric_history` filters by `dimension_value`.
- shape validator intersects past columns (mirrors pattern fix).
- Per-natural-key persistence dedup at the storage layer.

### P3 — Hygiene
- Storage `read_validation_history_bulk` (P4.3 single-round-trip).
- Notification routing per `(channel, key)` independence.
- HTML report + plot regression coverage.
- Dashboard auto-gen hook hardening.

## Files Changed

41 files; +4,787 / −708 lines. New tests:
`tests/test_findings_sweep.py` (461 cases),
`tests/test_storage/test_bulk_suppression.py` (425 cases),
plus regression coverage across models / wap / validate_df.

## Plan PR

[#6](https://github.com/amitranjan-oracle/qualifire/pull/6) —
`feat(findings-sweep): coordinated correctness + identity-keys sweep`
