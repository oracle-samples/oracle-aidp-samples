---
shipped: 2026-05-11
---

# Shipped: deck-content-port-to-repo-docs

## Summary

Ports evaluation-grade content from the unversioned pitch-deck
draft at `~/OracleContent/idl/data-quality/qualifire-overview/README.md`
into four repo doc surfaces so evaluators have a single
in-repo landing pad for "Why Qualifire", competitive
positioning, the validator chooser, and the visual
architecture mental model.

Pure-docs feature — no production code or tests changed
(beyond `docs-lint` continuing to pass).

## Key Changes

- **`README.md`** — new `## Why Qualifire` section between
  the opening summary and `## Key Features`. Contains a
  4-sentence motivation paragraph + a 5-row persona table
  (Data engineer / Pipeline owner / On-call / Compliance /
  SecOps / Platform team) with Before / With Qualifire
  columns + crosslinks to `docs/comparison.md`,
  `docs/validators/README.md`, `docs/architecture.md`. No
  mermaid in this section (README is the PyPI
  `long_description` per `pyproject.toml:9`; PyPI does not
  render mermaid).

- **`docs/comparison.md`** — new file (~150 LOC). Six
  required content sections in order: intro, landscape
  mermaid (in-compute vs SaaS), 7-row × 6-column capability
  table (Qualifire / Great Expectations / Deequ-PyDeequ /
  Soda Core OSS / dbt tests + dbt-expectations / Pandera /
  Monte Carlo-Bigeye-Anomalo), per-library prose, "What
  Qualifire is NOT trying to be" scope block, one-paragraph
  positioning statement, plus an optional terminal
  `## See also` navigation block.

- **`docs/validators/README.md`** — adds:
  - `## Complexity ladder` mermaid above the existing
    `## How to choose` chooser table (stateless → stateful).
  - `## Decision flowchart — drift vs shape vs pattern`
    mermaid below the chooser table.
  - `## Three concrete scenarios` with A / B / C
    (revenue ratio drop → drift; encoding migration →
    shape; correlated-columns ETL bug → pattern), each
    with a tradeoff resolution.
  - Tradeoff summary table covering all six validators.
  - `## See also` entry pointing to `docs/comparison.md`.

- **`docs/architecture.md`** — three new content blocks
  plus the opening scope-block fix:
  - **Scope-block fix at lines 6-12** — previously claimed
    "Three sections" and cited `architecture-backfill-loop-diagram`
    as a backlog item; both stale since PR #31 + PR #32.
    Now correctly lists the six sections (§0, §1, §2, §2a,
    §2b, §3) and drops the obsolete backlog reference.
  - **New `## 0. The four layers at a glance`** before
    `## 1.` — mermaid showing Engine → Collection →
    Validation → System table ⇄ Notification with the
    canonical class names per layer.
  - **New "Sequence view" subsection inside `## 1.`**
    between the existing ASCII trace and the storage-
    read-pattern subsection — mermaid sequence diagram
    rendering one `qf.validate(...)` call's actor
    interactions (User → Engine → Backend → Collector →
    Validator → System table → Notifier).
  - **New `## 2b. Stateless vs stateful validators`**
    between §2a (backfill loop) and §3 — mermaid showing
    the read-pattern split, plus a 5-column comparison
    table (Validators / Reads system table? / Cold-start
    behaviour / partition_ts required? / Replayable via
    backfill?) tying the split to `qf.backfill(...)`.

- **`docs/CHANGELOG.md`** — Unreleased / Documentation
  entry describing the four-surface port.

## Files Changed

5 files; +480 / -7 lines (excluding plan.md / shipped.md).

## Plan PR

[#34](https://github.com/amitranjan-oracle/qualifire/pull/34).

## Review Cycles

Plan: 2 codex rounds.
- R1: 3 BLOCKERs — stale `docs/architecture.md:6-12`
  scope block needs explicit fix step + AC; "paste into
  GitHub web editor" mermaid gate is not pre-merge
  verifiable; tone-calibration had no diff-verifiable AC.
- R2 PASS at v2 (after BLOCKER 4 fix added Section-D
  prerequisite + AC8b; BLOCKER 6 replaced with `mmdc`
  render-or-fail at `@mermaid-js/mermaid-cli@11.4.2`;
  BLOCKER 7 added AC13 banned-phrase grep).

Implementation: 3 codex rounds, **3 factual issues caught
before merge**.
- R1: 2 BLOCKERs —
  - Comparison table's Qualifire ML cell claimed
    "Prophet (forecast/trend), all with SHAP" — Prophet
    has no SHAP path; SHAP is scoped to shape (Isolation
    Forest) and pattern (RandomForest). Cell corrected.
  - `docs/comparison.md` has a 7th `## See also` section
    that AC2's strict six-section list didn't permit;
    AC2 amended (in plan v3) to allow an optional
    terminal navigation block.
- R2: 1 BLOCKER — the AC2 amendment cited
  `docs/configuration.md` as precedent for the optional
  `## See also`, but `docs/configuration.md` has no such
  heading. Real precedent is `docs/validators/README.md`
  + all 7 per-validator pages under `docs/validators/`.
  Citation corrected in plan v3.
- R3 PASS.

## Local Test Results

- `pytest tests/test_docs_links.py` — PASS, no broken
  internal markdown links introduced.
- `npx -p @mermaid-js/mermaid-cli@11.4.2 mmdc -i <block>
  -o /tmp/<n>.png` — all 6 new mermaid blocks render
  cleanly.
- Banned-phrase grep (AC13) — zero hits across the 4
  affected files for the marketing-vocabulary set
  (`best documented`, `best.in.class`, `industry.leading`,
  `vendor lock.in`, `on.call you can.t escape`, `\bshines\b`,
  `hit.piece`, `\bcrowded\b`, `egress required`).
