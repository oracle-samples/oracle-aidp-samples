---
shipped: 2026-05-11
---

# Shipped: configuration-reference-collectors-extension

## Summary

Closes the comprehensive-config-and-architecture-docs chain
by auditing the 4 remaining collector config surfaces in
`docs/configuration.md`. Together with the parent
`configuration-reference-audit` (10 surfaces) the reference
now covers all 14 user-facing config types and all nested
sub-types. `docs/architecture.md` §3 reduced to a one-line
"comprehensive coverage complete" pointer; no remaining
"what's NOT in this doc yet" backlog.

## Key Changes

- **`docs/configuration.md`** — 4 new
  `### <Type>CollectionConfig field reference` subsections
  added to the `## Collection Types` block:
  - `ProfilingCollectionConfig` + nested
    `ColumnProfileOverride` sub-table — compact field
    reference; deep examples remain in
    `## Profiling Collection`.
  - `MetricsCollectionConfig` — full template (field
    table + worked YAML + pitfalls).
  - `CustomQueryCollectionConfig` — full template; pitfalls
    quote the exact `_reject_nonempty_filter` config-load
    error (including the dynamic `Got: filter={raw!r}`
    suffix) and describe the two-path behavior (single-row
    drop vs. dimensional per-row emission).
  - `RecencyCollectionConfig` — field reference + explicit
    note that this type is NOT a standalone collector
    (no `type` field, not in the `CollectionConfig` union),
    only mounts under `SLOValidationConfig.recency`.
- Coverage footer updated from "10 audited surfaces" →
  "14 audited surfaces"; deferred-collectors paragraph
  removed.
- **`docs/architecture.md` §3** — reduced from a single
  bullet (this feature) to a one-line "comprehensive
  coverage complete; future features update these docs
  in their own impl ACs" pointer.
- **`docs/CHANGELOG.md`** — Documentation entry.

## Files Changed

3 files; +258 / −41 lines (excluding plan.md / shipped.md).

## Plan PR

[#32](https://github.com/amitranjan-oracle/qualifire/pull/32).

## Review Cycles

Plan: 2 codex rounds.
- R1: 2 BLOCKERs — `ColumnProfileOverride` nested
  sub-table was missing from the Profiling AC; standalone
  `RecencyCollectionConfig` per-strategy hard-fail error
  messages weren't explicitly required by AC2.
- R2 PASS at v2.

Implementation: 3 codex rounds, **7 factual errors caught
total** before merge.
- R1: 6 factual errors —
  - CustomQuery filter-rejection error quote was truncated
    (missing the dynamic `Got: filter={raw!r}.` suffix).
  - Profiling `dimensions` documented as GROUP BY but the
    engine doesn't pass the field to `ProfilingCollector`
    (silent no-op today).
  - Profiling `quantiles: []` pitfall claimed it disabled
    quantile computation; actually falls back to defaults
    via `self.quantiles or [...]`.
  - CustomQuery `dimensions` row described as "engine adds
    GROUP BY"; actually the engine emits the SQL verbatim
    and only the collector reads dim values from returned
    rows.
  - Standalone Recency section invented a `type: "recency"`
    collection block — `RecencyCollectionConfig` has no
    `type` field and is not in the `CollectionConfig`
    union; only mounts under SLO's `recency:`.
  - `metadata` strategy claimed Delta/Iceberg support;
    actually backend-dependent and Spark/pandas backends
    don't expose `last_modified`.
- R2: 1 factual error — CustomQuery sections still
  described multi-row results as "undefined / lossy",
  conflicting with the dimensional multi-row handling
  path in `custom_query.py:109-145`.
- R3 PASS.

## Local Test Results

- `tests/test_docs_links.py` passes — no broken internal
  markdown links introduced.
