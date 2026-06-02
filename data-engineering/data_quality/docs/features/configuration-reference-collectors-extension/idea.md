---
id: configuration-reference-collectors-extension
name: Configuration Reference — Profiling / Metrics / CustomQuery / Recency Collectors
type: Documentation
priority: P3
effort: Small
impact: Low
created: 2026-05-10
---

# Configuration Reference — Collectors Extension

## Problem Statement

The parent `configuration-reference-audit` backlog covers the
9 most-used config surfaces in
`docs/configuration.md` (DatasetConfig + 6 validation configs +
AggregationCollectionConfig + SampleCollectionConfig). The
remaining 4 collector surfaces — `ProfilingCollectionConfig`,
`MetricsCollectionConfig`, `CustomQueryCollectionConfig`, and
`RecencyCollectionConfig` — are out of scope for the parent.
This backlog item extends the audited reference to those 4.

## Scope

For each of the 4 collectors:
- One-paragraph "what this is" intro.
- Field table: name, type, default, accepted forms.
- 1-2 worked examples (minimal + realistic).
- Common pitfalls.

## Why Deferred

Captured during `comprehensive-config-and-architecture-docs`
v2 scope cut. Depends on parent
`configuration-reference-audit` shipping first (the audited
file's structure is the template).

## Affected Areas

- `docs/configuration.md` — extends to the 4 collectors.
