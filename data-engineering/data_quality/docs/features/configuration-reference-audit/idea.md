---
id: configuration-reference-audit
name: Configuration Reference — Audit + Extend Existing 665-line File
type: Documentation
priority: P2
effort: Medium
impact: Medium
created: 2026-05-10
---

# Configuration Reference — Audit + Extend

## Problem Statement

`docs/configuration.md` exists today as a 665-line partial
reference. Audit + extend to cover the 9 most-used config
surfaces in depth so operators can answer "what does this knob
do?" without reading source.

## Scope

Cover with field tables + worked examples + common pitfalls:

1. `DatasetConfig`
2. `ThresholdValidationConfig`
3. `HistoricalValidationConfig` (drift)
4. `ForecastValidationConfig`
5. `PatternValidationConfig`
6. `AnomalyDetectionValidationConfig`
7. `SLOValidationConfig`
8. `AggregationCollectionConfig`
9. `SampleCollectionConfig`

OUT of scope (separate backlog
`configuration-reference-collectors-extension`):
ProfilingCollectionConfig, MetricsCollectionConfig,
CustomQueryCollectionConfig, RecencyCollectionConfig.

## Why It Matters

- The recent runtime-flag features (`skip_recollection`,
  `skip_revalidation`, `skip_renotification`,
  `redacted_columns`, `drift_breakdown_by_slice`) added
  ~10 new fields without a single home for the operator-
  facing reference.
- `docs/configuration.md` has been growing organically; an
  audit pass aligns the structure across surfaces.

## Affected Areas

- `docs/configuration.md` — the file itself (audit + extend).
- `docs/CHANGELOG.md` — cross-link entries from the reference.

## Why Deferred

Captured during `comprehensive-config-and-architecture-docs`
v2 scope cut. The README + architecture overview are higher
leverage per LOC than auditing 665 lines of existing
reference; the user signalled "let's wait a bit" on this part
during planning sweep.
