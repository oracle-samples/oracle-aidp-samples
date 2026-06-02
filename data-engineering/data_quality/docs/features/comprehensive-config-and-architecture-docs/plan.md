# Plan: comprehensive-config-and-architecture-docs

## Goal

Ship a **focused minimum** documentation update that absorbs the
recently-shipped capabilities (the runtime skip-* flags, drift
explainer enhancements, column-name redaction, details_json
parity tests) into the front-of-house surfaces — README and a
short architecture overview. Defer the comprehensive
configuration reference to a separate iteration the user can
prioritize when ready.

## Locked Decisions (planner's recommendation, narrow scope)

1. **Two deliverables, not three** —
   - `README.md` refresh: update Key Features, install matrix,
     Quick Start; absorb a Best Practices section inline.
   - `docs/architecture.md` (new): an ~ASCII data-flow overview
     + the WAP lifecycle. ~400 LOC, not 1500.
2. **`docs/configuration.md` audit is OUT of scope** for this
   PR. (Codex R1 LOW — `docs/configuration.md` already exists
   as a 665-line partial reference; not a "deferred-create"
   but a "deferred-audit-and-extend".) The user signalled
   "let's wait a bit" on parts of this feature during planning
   sweep. The README + architecture doc are the most leverage
   per LOC; auditing + extending the existing 665-line
   reference benefits from being a separate iteration.
   **Captured as `configuration-reference-audit` backlog
   item** with the 9 target surfaces enumerated (DatasetConfig,
   the 6 validation configs, AggregationCollectionConfig,
   SampleCollectionConfig) so the user can pick it up next.
3. **Pydantic class docstring audit** is also OUT of scope —
   captured as `pydantic-docstring-audit` backlog (already
   referenced in earlier conversation; this PR formalises it).
4. **Backfill loop diagram** in architecture.md: out of scope.
   Captured as `architecture-backfill-loop-diagram` backlog —
   the data-flow + WAP diagrams cover the most-asked questions
   today.
5. **No automated doc lint tests.** Out of scope; captured as
   `docs-lint-ci` backlog.

## What Changes

### `README.md` (revised)

- Update Key Features bullets to mention all
  recently-shipped capabilities:
  - Runtime skip-* flags (`skip_recollection`,
    `skip_revalidation`, `skip_renotification`).
  - Per-past-slice drift breakdown.
  - Column-name redaction (HIPAA / GDPR / PCI safety).
  - Cross-backend `details_json` parity coverage.
  - Existing capabilities: signed drift, rate-of-change,
    partition tracking, descriptions, interactive dashboard,
    WAP lifecycle, backfill, soft-delete.
- Refresh install matrix (verify against `pyproject.toml`).
- Single Quick Start: a YAML config + a 5-line Python script
  + the expected ValidationResult shape.
- Inline Best Practices section (~100 lines) covering: when to
  use the runtime skip-* flags, dashboard cadence,
  history-backed validator warm-up, redaction policy
  configuration, and WAP for partial-day backfills.
- **CHANGELOG sweep** (codex R1 MAJOR): walk every
  `Breaking` entry currently in `docs/CHANGELOG.md` Unreleased
  section + recent releases; check the README does not
  contradict any. Specifically pinned today: notifier
  precedence (instance + YAML + programmatic; see
  `docs/programmatic_api.md`); the line at `README.md:308`
  claiming "YAML wins" needs correction.

### `docs/architecture.md` (new)

- One-page (~400 LOC) overview.
- Section 1: "How a value flows" — ASCII diagram tracing one
  `ThresholdValidationConfig` from YAML through (codex R1
  MAJOR — actual orchestration order verified at engine.py:475):
  YAML → `Qualifire.run_config_parsed` →
  `QualifireEngine._collect` → `AggregationCollector` →
  `CollectionResult` → `_validate` → `ThresholdValidator` →
  `ValidationResult` → `_prefetch_suppression_snapshot`
  (when `skip_renotification`) → `_persist_data_rows` →
  `_send_grouped_notifications` → `_persist_notification_rows`
  → system table → next-run history read.
- Section 2: "WAP lifecycle" — write → audit (validators run
  against staging) → publish-or-rollback. ASCII diagram.
- Section 3: pointers to all 5 backlog items
  (`configuration-reference-audit`, `pydantic-docstring-audit`,
  `architecture-backfill-loop-diagram`, `docs-lint-ci`,
  `configuration-reference-collectors-extension`) so a reader
  knows what's NOT here yet (codex R1 LOW — list of pointers
  matches the count of backlog idea.md files exactly).

### Backlog items to capture (5 idea.md files)

- `configuration-reference-audit`: audit + extend the existing
  665-line `docs/configuration.md` to cover the 9 target
  surfaces (DatasetConfig, the 6 validation configs,
  AggregationCollectionConfig, SampleCollectionConfig) with
  field tables, examples, and pitfalls.
- `pydantic-docstring-audit`: audit + tighten config-class
  docstrings so they read as primary source for the
  configuration reference.
- `architecture-backfill-loop-diagram`: backfill driver loop
  ASCII diagram in `docs/architecture.md`.
- `docs-lint-ci`: smoke test for broken internal markdown
  links.
- `configuration-reference-collectors-extension`: extends the
  audited config reference to Profiling / Metrics / CustomQuery
  / Recency collectors (parent depends on
  `configuration-reference-audit`).

## Acceptance Criteria

- AC1: `docs/architecture.md` exists with the 3 sections
  (data flow, WAP, pointers) and ~400 LOC.
- AC2: `README.md` Key Features bullets mention all
  capabilities in Locked Decision 1's "recently-shipped"
  list. Install matrix matches `pyproject.toml`.
- AC3: README's Quick Start can be pasted into a Python REPL
  and runs end-to-end without modification. Concrete
  verification (codex R1 MEDIUM): the impl author runs the
  literal commands from the README — (1) `python -m venv venv
  && source venv/bin/activate && pip install -e '.[pandas]'`,
  (2) write the YAML at the documented path, (3) run the
  literal Python snippet from the README — and copies the
  printed `QualifireResult.overall_severity` (codex R2 MEDIUM
  — the property lives on `QualifireResult` /
  `DatasetResult`, not `ValidationResult`) into the doc.
  The shipped.md captures the captured output as evidence.
- AC4: All 5 backlog `idea.md` files exist under
  `docs/features/<id>/idea.md` with frontmatter + the
  problem/scope/effort fields.
- AC5: No content in the new doc makes claims that contradict
  the code (verified by spot-grep of cited symbols / file
  paths during codex review).
- AC6: No new production-code changes.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Docs go stale on the next feature merge | Cross-link CHANGELOG; future-feature plans should update doc(s) as part of impl. Out of scope to enforce here. |
| Quick Start example breaks on a config schema change | Keep minimal (DatasetConfig + 1 ThresholdValidationConfig + 1 console notifier). |
| ASCII diagram drifts from real module paths | Reference concrete module paths so grep can validate; codex impl-review checks symbols still exist. |
| Scope creep (urge to document everything) | Locked Decision 2 carves out 5 backlog items. Any "should we add X?" question is checked against the carve-outs first. |

## Out-of-Band Reviews

- 2 adversarial plan reviews.
- 2 codex plan reviews → iterate to PASS.
- Then implement; 2 adversarial impl reviews + 2 codex impl reviews.

## Effort

Small-Medium (reduced from the v1 sketch). Estimated:
- ~400 LOC of `docs/architecture.md`.
- ~250 LOC of README diffs.
- ~5 backlog `idea.md` files, ~50 LOC total.

## Plan Iteration Log

- v1: initial draft (overscoped — 3 deliverables, ~2500 LOC).
- v4: addressed codex plan-review round 2 — MEDIUM:
  AC3 said "ValidationResult.overall_severity" but the
  property lives on `QualifireResult` / `DatasetResult` per
  `qualifire/core/models.py:135-214`. Corrected.
- v3: addressed codex plan-review round 1 —
  (a) MAJOR: data-flow ordering corrected (snapshot →
  persist → notify → persist_notification per engine.py:475);
  (b) MAJOR: README CHANGELOG sweep added (notifier
  precedence + other shipped breaking notes);
  (c) MEDIUM: AC3 verification step concretised (literal
  commands + captured output in shipped.md);
  (d) LOW: `docs/configuration.md` already exists (665 lines);
  reframed backlog as "audit + extend," not
  "create-from-scratch";
  (e) LOW: architecture pointers list aligned with the
  count of backlog idea.md files (5).
- v2: scope cut — drop `docs/configuration.md` to backlog;
  README + architecture.md only. The user signalled "wait a
  bit" on parts of this feature during the planning sweep,
  and the comprehensive reference is the most labor-intensive
  surface; deferring it to a focused iteration the user can
  prioritize is the right move.
