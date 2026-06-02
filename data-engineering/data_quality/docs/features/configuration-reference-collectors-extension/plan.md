# Plan: configuration-reference-collectors-extension

## Goal

Extend `docs/configuration.md` (currently ~1100 lines post the
`configuration-reference-audit` ship) to cover the remaining 4
collector surfaces deferred from that audit:
`ProfilingCollectionConfig`, `MetricsCollectionConfig`,
`CustomQueryCollectionConfig`, and standalone
`RecencyCollectionConfig`. Each gets the same shape the parent
audit used for `Aggregation` / `Sample`: field-reference table
(name / type / default / accepted forms), worked YAML example,
and a "Common pitfalls" subsection.

This is the LAST remaining bullet in
`docs/architecture.md` §3 ("What's NOT in this doc yet").
Shipping it closes the comprehensive-config-and-architecture-docs
chain entirely.

## Locked Decisions

1. **Scope: 4 collectors.** No new code; doc-only audit.
   - `ProfilingCollectionConfig` — extend the existing
     `## Profiling Collection` section (currently lines
     926-996) with the audit-pattern field table; preserve
     the existing prose subsections (Stats by column type,
     Global stat filtering, Per-column overrides,
     Precedence rules) verbatim because they're already
     high-quality reference material.
   - `MetricsCollectionConfig` — NEW section, full template.
   - `CustomQueryCollectionConfig` — NEW section, full
     template. Must cover the `filter:` rejection
     (`_reject_nonempty_filter` validator) because operators
     migrating from pre-2026-05 configs will hit this.
   - `RecencyCollectionConfig` (standalone) — NEW section
     covering the 4 strategies. Cross-references the
     existing SLO-embedded recency table at lines 397-405
     (preserved verbatim) instead of duplicating it; the new
     section's role is "what does standalone recency
     collection look like when NOT inside an SLO validation"
     and to clarify the `column` / `sql` per-strategy
     requirements via the `_validate_strategy_fields`
     model-validator.

2. **Insertion site.** All 4 new tables go into the
   existing `## Collection Types` section block (currently
   ends at line 873 after `SampleCollectionConfig`).
   Order: `Aggregation` → `Sample` (existing) →
   `Profiling` (new field ref; existing deep prose stays
   in `## Profiling Collection` further down) → `Metrics`
   → `CustomQuery` → `Recency`. This keeps every collector
   field table in one place — what `## Collection Types`
   already promised at the top-of-section table at
   line 813.

3. **Update the Coverage footer.** The footer at lines
   875-900 currently lists the 4 remaining collectors as
   deferred. After this ship, the footer reads "all 14
   audited" (10 from parent + 4 from this).

4. **Update `docs/architecture.md` §3.** After this ship,
   §3 (currently the single bullet for this feature, post
   the `architecture-backfill-loop-diagram` cleanup) goes
   to ZERO bullets. The whole §3 heading can be removed,
   or replaced with a one-line "comprehensive coverage
   complete; future features update this doc in their
   own PRs" pointer. Pick the one-liner because removing
   §3 entirely would change the §1 / §2 / §2a numbering
   contract.

5. **Verify field accuracy by spot-grep** against
   `qualifire/core/config.py` — same protocol that caught
   18 factual errors during the parent audit. Every field
   in every new table must match the actual class
   definition.

## What Changes

### `docs/configuration.md`

- **New subsection inside `## Collection Types`** after
  `### SampleCollectionConfig field reference` (before
  the `## Coverage of this reference` heading):
  - `### ProfilingCollectionConfig field reference` —
    field table + 2-3 pitfall bullets. Cross-links to
    `## Profiling Collection` (already deep) for the
    Stats-by-column-type matrix.
  - `### MetricsCollectionConfig field reference` —
    field table + worked YAML + 2-3 pitfall bullets.
    Cross-link to `MetricsCollector` semantics (it's the
    same shape as Aggregation; pitfalls cover the
    name-collision risk and when to use Metrics vs
    Aggregation).
  - `### CustomQueryCollectionConfig field reference` —
    field table + worked YAML + 2-3 pitfall bullets.
    Must call out the `filter:` rejection.
  - `### RecencyCollectionConfig field reference
    (standalone)` — field table + per-strategy required
    fields + 2-3 pitfall bullets. Cross-link to SLO's
    embedded recency at line 397.

- **Coverage footer update** (lines 875-900) — change
  "10 most-used scope items" → "14 audited scope items",
  drop the "Not covered in this audit" para, replace with
  a "Future field additions update both code + this
  reference in the same PR per AC" line.

### `docs/architecture.md`

- Section §3 reduced to a one-liner: "Comprehensive
  coverage complete. Future features update
  `docs/architecture.md` / `docs/configuration.md` as part
  of their own impl ACs."

### `docs/CHANGELOG.md`

- New entry under Unreleased / Documentation describing
  the 4 collector audits.

### Backlog cleanup

- `docs/features/configuration-reference-collectors-extension/shipped.md`.

## Acceptance Criteria

### Diff-verifiable

- AC1: 4 new `### <Type>CollectionConfig field reference`
  subsections exist in `docs/configuration.md` under
  `## Collection Types`, after the existing Sample
  subsection and before the Coverage footer.
- AC2: Each of the 4 subsections has a field table with
  exactly the fields from the matching Pydantic class in
  `qualifire/core/config.py` — no fabricated fields, no
  fabricated defaults.
  - `ProfilingCollectionConfig` (config.py:317-340):
    `type`, `columns`, `stats`, `exclude_stats`,
    `filter`, `top_k`, `quantiles`, `column_profiles`,
    `dimensions`. Plus a nested `ColumnProfileOverride`
    sub-table (config.py:310-314) covering the
    `stats` / `exclude_stats` fields so the
    `column_profiles: dict[str, ColumnProfileOverride]`
    accepted form is fully audited.
  - `MetricsCollectionConfig` (config.py:343-361):
    `type`, `metrics`, `filter`, `dimensions`.
  - `CustomQueryCollectionConfig` (config.py:476-527):
    `type`, `sql`, `dimensions`. NOTE: `filter` is
    rejected by `_reject_nonempty_filter` — must NOT
    appear in the field table; must appear in the
    pitfalls section instead.
  - `RecencyCollectionConfig` (config.py:214-225):
    `strategy`, `column`, `sql`. The per-strategy
    required-field rules from
    `_validate_strategy_fields` (config.py:219-225)
    must appear in the table or in the pitfalls
    (whichever is clearer) — including the exact error
    messages `"'column' is required for max_column
    strategy"` and `"'sql' is required for custom_sql
    strategy"`.
- AC3: Each new subsection has a "Common pitfalls"
  block with at least 2 bullets.
- AC4: 3 of the 4 subsections (all except Profiling) have
  at least one worked YAML example. Profiling's example
  remains in the deep `## Profiling Collection` section;
  the new field-reference subsection cross-links there.
- AC5: Coverage footer updated — no longer lists any
  collector as deferred.
- AC6: `docs/architecture.md` §3 contains a one-line
  pointer (no remaining backlog bullets).

### Runtime-gate

- AC-Run-1: `pytest tests/test_docs_links.py` still
  passes — no broken internal markdown links.
- AC-Run-2: Codex impl review on field-by-field factual
  accuracy.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Factual errors slip through (parent audit caught 18) | Spot-grep every field name + default against `qualifire/core/config.py` before claiming PASS. |
| Profiling field table duplicates the existing deep section's content | Keep the field table COMPACT; defer narrative to the existing `## Profiling Collection`. Cross-link. |
| §3 one-liner reads awkwardly with §1 / §2 / §2a numbering | If awkward, leave §3 as a single ZERO-bullet line + paragraph. Decide at impl time; AC6 only requires "one-line pointer, no remaining bullets." |
| CustomQuery's filter-rejection ergonomics is non-obvious | Pitfalls section MUST link to `_reject_nonempty_filter` (config.py:485-527) and quote the actual error message. |

## Out-of-Band Reviews

- 1 codex plan review.
- 1-2 codex impl reviews (field-by-field accuracy).

## Effort

Small. ~150 LOC added to `docs/configuration.md` + 1-line
change to `docs/architecture.md` + CHANGELOG entry.

## Plan Iteration Log

- v1: initial draft.
- v2: addressed codex plan-review R1 — added
  `ColumnProfileOverride` nested sub-table requirement to
  AC2's Profiling line (R1 BLOCKER 1); expanded AC2's
  Recency line to require quoting the exact per-strategy
  hard-fail error messages from
  `_validate_strategy_fields` (R1 BLOCKER 2).
