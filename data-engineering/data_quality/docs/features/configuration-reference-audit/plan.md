# Plan: configuration-reference-audit

## Goal

Audit + extend the existing 665-line `docs/configuration.md`
so each of the 9 priority Pydantic config surfaces has:
field table (name / type / default / accepted forms),
worked example, common pitfalls. Cross-link to the docstrings
that shipped in `pydantic-docstring-audit` as primary source.

## Locked Decisions

1. **Scope: 9 priority surfaces + their required sub-types
   + QualifireConfig root**. Matching the parent backlog
   item's list, with sub-types covered inline:
   - `QualifireConfig` (NEW — codex R1 finding: root fields
     `owner` / `bu` / `system_table` / `system_table_backend` /
     `jdbc` / `dataset_parallelism` / `context` /
     `notifications` / `datasets` / `engine_notify` /
     `partition_ts` / `partition_step` are the operator's
     first encounter with the schema; can't omit).
   - `DatasetConfig` — including the source-mutex (`table` /
     `query` / `df` / `wap`) explicit callout.
   - `ThresholdValidationConfig` — including inline
     `ThresholdLevels` + `ThresholdBounds` + `ThresholdRuleConfig`
     vocabulary (a shared "Threshold bounds vocabulary"
     mini-section since Threshold + the per-measure forms
     reuse it).
   - `HistoricalValidationConfig` (drift) — including inline
     `HistoricalRuleConfig` + `HistoricalCompareConfig` +
     `HistoricalThresholds`.
   - `ForecastValidationConfig` — including inline
     `ForecastRuleConfig` + `ForecastModelConfig`.
   - `PatternValidationConfig` — including inline
     `PatternModelConfig` + `PatternThresholdConfig`.
   - `AnomalyDetectionValidationConfig` — including inline
     `AnomalyModelConfig` + `AnomalyThresholdConfig`.
   - `SLOValidationConfig` — including inline
     `RecencyCollectionConfig` (SLO requires it; even though
     RecencyCollectionConfig itself is in the deferred
     `collectors-extension` backlog, the SLO reader needs
     enough to write a working SLO config).
   - `AggregationCollectionConfig`.
   - `SampleCollectionConfig` — including inline
     `SampleHistoryConfig`.

   The 4 less-common standalone collectors (Profiling /
   Metrics / CustomQuery / Recency-as-standalone) remain in
   `configuration-reference-collectors-extension`.

   **Notification channel configs already covered by the
   existing `## Notification Channels` section** (line 166
   today): `EmailNotificationConfig` (line 168),
   `SlackNotificationConfig` (line 185),
   `WebhookNotificationConfig` (line 194),
   `ConsoleNotificationConfig` (in the same block — Console
   has no fields beyond `type`). All four are preserved
   verbatim. The per-validation `notify` field in each
   validation-config table cross-links to that section
   ("see `## Notification Channels`") so readers landing on
   a validator's table know where to find channel shapes.

2. **Per-section shape** (extend existing where present):
   - One-paragraph intro that cross-links to the class
     docstring in `qualifire/core/config.py`.
   - Field table: `| field | type | default | accepted forms |`.
   - One worked YAML example (the smallest legal config OR
     a realistic one — pick whichever is more useful).
   - "Common pitfalls" subsection (2-4 bullets).

3. **Keep existing content where it's still accurate.**
   Some sections (Profiling, JDBC, External Catalog) have
   good standalone material; don't disturb them. Audit pass
   focuses on the 9 surfaces; everything else is preserved
   verbatim.

4. **Verify field accuracy** by spot-grepping each documented
   field against the source class (caught 10 factual errors
   in the docstring audit; same risk applies here). Field
   tables MUST match the actual Pydantic class fields +
   defaults + types.

5. **No new fields documented.** This is a reference for
   what exists today; future features update the reference
   as part of their own impl ACs.

## What Changes

### `docs/configuration.md` — 10 section audits (9 priority surfaces + QualifireConfig root)

For each of the 10 covered scope items:
- If a section exists today: extend with field table + pitfalls;
  preserve existing prose where accurate.
- If a section is missing or thin: add the full template.

Specific surfaces today (per `grep -n "^### " docs/configuration.md`):
- ✅ `### SLO Check` (line 289) — exists, audit
- ✅ `### Threshold Check` (line 310) — exists, audit
- ✅ `### Historical Comparison` (line 335) — exists, audit
- ✅ `### Forecast (Prophet)` (line 358) — exists, audit
- ✅ `### Anomaly Detection (Isolation Forest + SHAP)` (line 382) — exists, audit
- ✅ `### Pattern Check (Random Forest two-sample + SHAP)` (line 407) — exists, audit
- ✅ `## Dataset Config` (line 206) — exists, audit
- ⚠️ `## Collection Types` (line 450) — needs `AggregationCollectionConfig` + `SampleCollectionConfig` subsections with field tables.
- ⚠️ `## Profiling Collection` — already deep; out of scope (covered by collectors-extension backlog).

### Backlog reference at the bottom

Add a "Coverage" section noting:
- The 9 covered surfaces.
- What's deferred to `configuration-reference-collectors-extension`.
- Pointer to `pydantic-docstring-audit`'s class docstrings as
  the in-code primary source.

## Acceptance Criteria

- AC1: Each of the 10 covered scope items (9 priority
  surfaces + `QualifireConfig` root) has a field table
  (name / type / default / accepted forms).
- AC2: Each of the 10 has at least one worked YAML example
  (smallest legal config OR realistic).
- AC3: Each of the 10 has a "Common pitfalls" subsection with
  at least 2 bullets.
- AC4: No factual errors — each documented field matches
  the actual Pydantic class field (spot-grep verified).
- AC5: Existing valid content preserved verbatim where
  feasible.
- AC6: `tests/test_docs_links.py` still passes (no broken
  internal links introduced).
- AC7: Coverage footer captures what's NOT in this doc yet.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Field tables drift from code over time | Cross-link to class docstrings (just shipped); future field changes should update both code + docstring + reference in the same PR. Out of scope to enforce here. |
| Factual errors slip through (docstring audit caught 10) | Codex impl review focused on field-by-field accuracy; spot-grep every field name in the audit. |
| Audit grows the file by 2× and becomes unwieldy | Keep field tables compact; one-line entries; defer narrative to per-section "Common pitfalls" + downstream docs. Target +400-600 LOC total, not 2k. |

## Out-of-Band Reviews

- 1 codex plan review.
- Then implement; 1-2 codex impl reviews (focused on field-
  level factual accuracy).

## Effort

Medium. ~400-600 LOC of additions to `docs/configuration.md`.

## Plan Iteration Log

- v1: initial draft.
- v3: addressed codex plan-review round 2 — section count
  rewording ("9" → "10" everywhere covering audit + ACs);
  notification channel configs now explicitly named
  (Email / Slack / Webhook / Console) with line refs.
- v2: addressed codex plan-review round 1 — expanded scope
  to include `QualifireConfig` root fields (operator's first
  schema encounter; can't omit), and named the required
  sub-types per validation surface (rules / compare / model /
  recency / history blocks) that must be covered inline.
  Notification channel configs remain covered in the
  existing `## Notification Channels` section (preserved
  verbatim).
