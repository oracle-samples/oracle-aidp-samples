# Plan: deck-content-port-to-repo-docs

## Goal

Port the evaluation-grade content from the unversioned deck draft
at `~/OracleContent/idl/data-quality/qualifire-overview/README.md`
into repo docs so a Qualifire evaluator has a single in-repo
landing surface for "Why Qualifire", "How is it different from
GE / Deequ / Soda / Pandera / SaaS-DQ-vendors", "Which validator
do I want?", and "What does the four-layer architecture look like
visually?".

Four discrete content additions: **(A)** README "Why Qualifire"
header, **(B)** new `docs/comparison.md`, **(C)** mermaid +
scenarios in `docs/validators/README.md`, **(D)** four-layer
diagram + sequence diagram + stateless/stateful section in
`docs/architecture.md`.

Pure-docs feature — no production code, no test changes (beyond
the docs-lint test that must keep passing).

## Locked Decisions

1. **Tone calibration: neutral, not deck-marketing.** Strip the
   deck's marketing-y framings ("the best documented DQ library",
   "vendor on-call you can't escape", "egress required"). Keep
   factual statements ("Great Expectations ships >50 expectations
   with Data Docs"; "Monte Carlo is vendor-hosted and queries
   your warehouse, so traffic egresses your VPC"). The one-line
   positioning at the bottom of the comparison page is the only
   place a single sentence of opinionated phrasing survives —
   and even there, drop the "to AIDP what Deequ was..." analogy
   in favor of a flat "what Qualifire is" statement.

2. **`docs/comparison.md` placement: directly under `docs/`.**
   Not under a marketing/ or evaluation/ subfolder. Reasons:
   - Single-page port; no follow-on evaluation content is
     planned, so a folder is over-engineering.
   - `docs/CHANGELOG.md`, `docs/architecture.md`,
     `docs/configuration.md` all live at the same level —
     `docs/comparison.md` joins them without breaking the
     mental model "every top-level doc is a top-level concept".

3. **Mermaid usage by surface:**
   - `README.md` (Section A): NO mermaid. README.md is the PyPI
     `long_description` (`pyproject.toml:9: readme = "README.md"`)
     and PyPI's renderer does not render mermaid — it would
     ship a literal `mermaid` code-fence to every PyPI viewer.
     Section A is prose + a markdown table only.
   - `docs/comparison.md` (Section B): YES — landscape diagram
     (in-compute vs SaaS). GitHub renders mermaid; this doc is
     never bundled into PyPI.
   - `docs/validators/README.md` (Section C): YES — complexity
     ladder + decision flowchart. Existing chooser table stays
     as the text-only canonical fallback per the idea's open
     question on rendering surfaces.
   - `docs/architecture.md` (Section D): YES — four-layer
     decomposition + sequence diagram, ADDED before / after the
     existing ASCII traces (not replacing them).

4. **Architecture section ordering.** Today: `## 1. How a value
   flows` → `## 2. WAP lifecycle` → `## 2a. Backfill loop` →
   `## 3. What's NOT in this doc yet`. The new mermaid
   four-layer decomposition slots in as a NEW `## 0. The four
   layers at a glance` immediately after the existing opening
   prose. The new sequence diagram is appended to `## 1. How a
   value flows` as a "Sequence view" subsection (the existing
   ASCII flow is the canonical detail; sequence diagram is the
   visual complement). Stateless/stateful is a NEW `## 2b.
   Stateless vs stateful validators` between current §2a
   (Backfill loop) and `## 3`.

5. **README size budget: insert only.** No precondition cut to
   the install matrix or any other existing section. The
   "Why Qualifire" header is ~25-35 LOC net add. The README
   total grows from 720 LOC to ~750-760 LOC. If a future
   feature wants to fold the install matrix into a separate
   `docs/installation.md`, that's its own scope.

6. **Persona table cell content sourced from the deck verbatim
   where the wording is already neutral.** Where the wording is
   marketing-y (e.g. "no hosted UI, no vendor on-call"),
   rephrase to descriptive ("operates on local compute; no
   SaaS console") for the repo doc.

7. **"What Qualifire is NOT trying to be" stays.** Schema
   enforcement (use Pandera/dbt), lineage/catalog (use
   OpenLineage/Marquez/AIDP catalog), SaaS observability (no
   hosted UI). These are factual scope statements, not
   marketing — they keep operators from filing bug reports for
   features that are out of scope by design.

8. **Cross-link wiring.** Every new doc gets backlinks:
   - `README.md` "Why Qualifire" → links to `docs/comparison.md`,
     `docs/validators/README.md` (for "which validator do I
     want?"), and `docs/architecture.md` (for "how does it
     work?").
   - `docs/comparison.md` → links back to README, validators
     README, architecture.
   - `docs/validators/README.md` "See also" gets a
     `docs/comparison.md` entry.

9. **Validator-name alignment.** The deck uses "shape" and
   "pattern"; the repo's `docs/validators/README.md` already
   uses those names. Don't introduce any deck-only synonyms.
   Validator-type strings in YAML (`anomaly_detection`,
   `pattern`) are NOT exposed in the new diagrams — they
   reference validator pages by file name only.

10. **Source-of-truth check for every comparison-page claim.**
    The deck was written by reading code and `docs/` in May
    2026; six months out, some claims may have drifted. The
    impl pass spot-checks every concrete factual statement in
    the comparison page against the current repo:
    - Storage backends ("4 backends: external_catalog, delta,
      sqlite, jdbc") → verify against `qualifire/storage/`.
    - Notification channels ("email, Slack, webhook, console")
      → verify against `qualifire/notification/`.
    - WAP first-class → verify
      `Qualifire.write_audit_publish(...)` exists.
    - Skip flags ("three runtime gates: skip_recollection,
      skip_revalidation, skip_renotification") → verify
      against `qualifire/api.py` signatures.
    - ML detector list ("Isolation Forest, RandomForest
      two-sample, Prophet, all with SHAP") → verify against
      `qualifire/validation/`.

## What Changes

### `README.md` — Section A

Insert a new `## Why Qualifire` section between current line 10
(end of opening paragraph) and current line 12 (`## Key Features`).

Content:
- 3-4 sentence prose intro (modern data platforms ship in hours,
  the failure mode is silent corruption, hand-rolled asserts + per-
  team conventions leave a gap — Qualifire is the AIDP-native DQ
  layer that gives every team identity, history, severity,
  notification, and audit as a shared substrate).
- "Impact on users" persona table (5 rows: Data engineer, Pipeline
  owner, On-call, Compliance / SecOps, Platform team) with Before
  / With Qualifire columns. Source: deck draft lines 67-77; tone
  sanded down to neutral.
- One-line trailing pointer:
  `For a comparison against GE / Deequ / Soda / dbt tests /
  Pandera / SaaS DQ vendors, see [docs/comparison.md](docs/comparison.md).`

Target: 25-40 LOC net add.

### `docs/comparison.md` — Section B (NEW FILE)

Top-down structure:

1. **Intro** (3-4 sentences). Why this page exists (evaluator-
   facing positioning, not a hit-piece on other tools).
2. **Landscape mermaid diagram** — `in_compute` vs `saas`
   subgraphs, color-coded. Source: deck §12 mermaid.
3. **Side-by-side capability table** — 7 columns × 7 tools
   (Qualifire, Great Expectations, Deequ/PyDeequ, Soda Core
   OSS, dbt tests + dbt-expectations, Pandera, Monte Carlo /
   Bigeye / Anomalo). Columns: Runtime, History store, ML /
   multivariate, WAP / staging, Notifications, AIDP fit.
   Source: deck §12 table. Tone neutral.
4. **Per-library "where it shines vs gets in the way"** prose
   blocks (one paragraph per peer tool, neutral tone). Source:
   deck §12, rephrased.
5. **"What Qualifire is NOT trying to be"** — schema
   enforcement, lineage/catalog, SaaS observability product.
6. **One-line positioning statement** at bottom (rephrased
   from deck §12's "Qualifire is to AIDP what Deequ was..."
   into a flat self-statement).

Target: 180-260 LOC.

### `docs/validators/README.md` — Section C

Insert above the existing `## How to choose` heading:
- New subsection `## Complexity ladder` with mermaid:
  `slo → threshold → drift → forecast → shape → pattern`
  with stateless/stateful annotations.

Insert below the existing chooser table (between table at line
42 and `## Programmatic vs YAML` at line 44):
- New subsection `## Decision flowchart — drift vs shape vs
  pattern` with mermaid flowchart. Source: deck §3 lines
  393-407.
- New subsection `## Three concrete scenarios`:
  - Scenario A: Daily revenue ratio drops 30% → drift wins.
  - Scenario B: Customer migration changes encoding → shape
    wins.
  - Scenario C: ETL bug correlates two columns → pattern
    wins.
  Each ~6-8 lines, one tradeoff resolution per scenario.
  Source: deck §3 lines 411-440.

Add to the existing `## See also` list:
- `[docs/comparison.md](../comparison.md)` — "How Qualifire
  compares to other DQ libraries."

Target: 60-100 LOC net add.

### `docs/architecture.md` — Section D

**Stale-scope-block fix (R1 BLOCKER 4 prerequisite).** Lines
6-12 today read "Three sections — end-to-end value flow, the
WAP lifecycle, and pointers to deferred sections. ... The
backfill driver loop diagram is captured as the
`architecture-backfill-loop-diagram` backlog item." That
sentence has been stale since the backfill-loop port shipped
in PR #31 (the backlog item became live §2a) AND the
collectors-extension port reduced §3 to a one-liner in PR
#32. This feature MUST update the scope block to:
- count the post-port sections accurately (`## 0.`,
  `## 1.`, `## 2.`, `## 2a.`, `## 2b.`, `## 3.`),
- remove the stale "backlog item" sentence,
- name the four-layer overview, sequence diagram, and
  stateless-vs-stateful split as the new contents.

Then three new content additions:

1. New `## 0. The four layers at a glance` section between
   the existing opening prose (line 10) and current `## 1.`
   (line 14). Content: short paragraph + mermaid four-layer
   flowchart (Engine + Collection + Validation + Notification
   + SystemTable). Source: deck §2 lines 81-140. ~30-40 LOC.

2. New "Sequence view" subsection appended to current `## 1.
   How a value flows`. A mermaid sequence diagram showing one
   `qf.validate(...)` call: User → Engine → Backend →
   Validator → SystemTable → Notifier. Source: deck §2.3
   lines 173-201. ~25-35 LOC. Slots between current `### Why
   sample validators surface as "skipped"` subsection
   (preserving existing prose) — actually, between the
   existing ASCII trace at line 113 and the existing storage-
   read-pattern subsection at line 116. Reviewer-checkable:
   the sequence diagram precedes the storage-read narrative.

3. New `## 2b. Stateless vs stateful validators` section
   between current `## 2a. Backfill loop` (ends ~line 364)
   and `## 3. What's NOT in this doc yet` (line 365).
   Content: mermaid (stateless flow vs stateful flow with
   system-table side channel) + a table (Validators / Reads
   system table? / Cold-start behaviour / partition_ts
   required? / Replayable?) + a short prose para tying the
   split to `qf.backfill()`. Source: deck §4 lines 449-494.
   ~40-60 LOC.

### `docs/CHANGELOG.md`

New entry under `## Unreleased / Documentation` (alongside the
existing comprehensive-config-and-architecture-docs chain
entries) describing the four-surface port:

- Section A — README "Why Qualifire"
- Section B — `docs/comparison.md` new file
- Section C — validators chooser enhancements
- Section D — architecture four-layer + sequence + stateless/
  stateful

## Acceptance Criteria

### Diff-verifiable

- **AC1**: `README.md` contains a `## Why Qualifire` heading
  inserted between the opening prose and `## Key Features`,
  with the persona table (5 rows: Data engineer / Pipeline
  owner / On-call / Compliance / SecOps / Platform team) and
  a `docs/comparison.md` cross-link. NO mermaid in this
  section (PyPI rendering constraint).
- **AC2**: `docs/comparison.md` exists and contains these
  six required sections in this order: intro, landscape
  mermaid, side-by-side capability table, per-library
  prose, "What Qualifire is NOT trying to be", one-line
  positioning. An optional terminal `## See also`
  navigation block is permitted (consistent with the
  established convention in `docs/validators/README.md`
  and every per-validator page under `docs/validators/`)
  and does not count as a seventh "content" section.
- **AC3**: The capability table in `docs/comparison.md` lists
  exactly these 7 tools as rows: Qualifire, Great
  Expectations, Deequ/PyDeequ, Soda Core OSS, dbt tests +
  dbt-expectations, Pandera, Monte Carlo / Bigeye / Anomalo.
  Columns: Runtime, History store, ML/multivariate, WAP/
  staging, Notifications, AIDP fit.
- **AC4**: `docs/validators/README.md` has TWO new mermaid
  blocks: a "Complexity ladder" before the existing chooser
  table, and a "Decision flowchart" after it. The existing
  text-only chooser table at lines 35-42 is unchanged.
- **AC5**: `docs/validators/README.md` has a "Three concrete
  scenarios" subsection with EXACTLY 3 scenarios labelled A
  (drift wins), B (shape wins), C (pattern wins).
- **AC6**: `docs/architecture.md` has a new `## 0. The four
  layers at a glance` section BEFORE current `## 1.`. The
  existing `## 1. How a value flows` opening + ASCII flow is
  unchanged through line 113.
- **AC7**: `docs/architecture.md` `## 1.` block contains a
  new "Sequence view" mermaid sequence diagram inserted
  between the existing ASCII flow and the existing
  "Storage read pattern" subsection.
- **AC8**: `docs/architecture.md` has a new `## 2b.
  Stateless vs stateful validators` section between current
  §2a and §3, with a mermaid + table + prose.
- **AC8b** (R1 BLOCKER 4): `docs/architecture.md` lines 6-12
  scope block is updated — no longer says "Three sections",
  no longer cites `architecture-backfill-loop-diagram` as a
  backlog item, names the four-layer overview / sequence
  diagram / stateless-vs-stateful split as part of the
  document's scope.
- **AC9**: Every new mermaid block uses the standard
  ` ```mermaid ` fence (not ` ```mermaid graph ` or any
  variant) so GitHub renders it consistently.
- **AC10**: Factual cross-check — the new docs name only
  symbols that exist in the current code:
  - Storage backend names: `external_catalog`, `delta`,
    `sqlite`, `jdbc` (verify via `ls qualifire/storage/`).
  - Notifier names: email, slack, webhook, console (verify
    via `ls qualifire/notification/`).
  - WAP API: `Qualifire.write_audit_publish(...)` (verify
    in `qualifire/api.py`).
  - Skip flag names: `skip_recollection`,
    `skip_revalidation`, `skip_renotification` (verify in
    `qualifire/api.py` signatures).
  - Validator names: SLO, threshold, drift, forecast,
    shape, pattern (verify they're in
    `docs/validators/README.md`'s existing table).
- **AC11**: Internal markdown cross-links resolve (the new
  README → comparison link, comparison → validators link,
  validators README → comparison "See also" link). Verified
  by AC-Run-1.
- **AC12**: `docs/CHANGELOG.md` Unreleased / Documentation
  carries one new entry naming the four-surface port.
- **AC13** (R1 BLOCKER 7 — tone calibration): A
  `grep -Eni '<banned-phrase-set>' README.md docs/comparison.md
  docs/validators/README.md docs/architecture.md` returns
  ZERO hits, where the banned-phrase set is the marketing
  vocabulary Locked Decision 1 rules out:
  - `best documented`
  - `best.in.class`
  - `industry.leading`
  - `vendor lock.in`
  - `on.call you can.t escape`
  - `\bshines\b` (the deck's "where it shines vs gets in
    the way" framing)
  - `hit.piece`
  - `\bcrowded\b` (the deck's "DQ landscape is crowded")
  - `egress required`
  Diff-verifiable; impl phase greps before commit.

### Runtime-gate

- **AC-Run-1**: `pytest tests/test_docs_links.py` passes —
  no broken internal markdown links introduced.
- **AC-Run-2**: Codex impl review verifies every factual
  claim in `docs/comparison.md` against the current repo
  (per Locked Decision 10) AND confirms the comparison /
  validators / architecture additions read as neutral prose
  (not marketing).
- **AC-Run-3** (R1 BLOCKER 6 — mermaid syntax gate):
  Every new mermaid block in `docs/comparison.md`,
  `docs/validators/README.md`, and `docs/architecture.md`
  renders without error under
  `npx -p @mermaid-js/mermaid-cli@11.4.2 mmdc -i <block> -o /tmp/<n>.png`.
  Impl phase extracts each mermaid block to a temp file
  and runs the CLI; non-zero exit on any block is a
  hard-fail, fix the syntax before commit. (No PNG is
  committed; the gate is the render-or-fail.)

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Mermaid in `docs/validators/README.md` ships unrendered to PyPI | Mermaid is in `docs/`, NOT in README.md (the PyPI long_description). PyPI viewers never see those files. Verified via `grep readme pyproject.toml`. |
| Comparison page becomes a hit-piece on other tools | Locked Decision 1 + 6 enforce neutral tone. AC-Run-2 codex review explicitly checks this. |
| Deck factual claims have drifted since May 2026 | Locked Decision 10 + AC10 + AC-Run-2 spot-check every concrete factual claim against the current repo. |
| Mermaid syntax errors (broken on GitHub render) | AC-Run-3 enforces `mmdc` (mermaid-cli) render-or-fail on every new diagram before commit. |
| Architecture doc grows past readable length | Section D is ~85-135 LOC; current `docs/architecture.md` is ~370 LOC. Post-port ~455-505 LOC. Still well under e.g. `docs/configuration.md` (~1150 LOC). |
| `## 0.` numbering reads oddly relative to existing `## 1./2./2a./2b./3.` | Locked Decision 4 places the new section as `## 0.` deliberately — it's a 30-second overview that precedes the deep dive in §1. Reviewers can re-litigate at plan-review time if they hate it. |

## Out-of-Band Reviews

- 1 codex plan review (focus on scope completeness + tone
  calibration + factual-accuracy gate definition).
- 1-2 codex impl reviews (focus on every factual claim
  matching repo reality, mermaid renders, AC11 link
  resolution).

## Effort

Medium. Estimated:
- Section A: ~30 LOC to `README.md`.
- Section B: ~220 LOC new `docs/comparison.md`.
- Section C: ~80 LOC net add to `docs/validators/README.md`.
- Section D: ~110 LOC net add to `docs/architecture.md`.
- CHANGELOG: ~12 LOC.
- Total: ~450 LOC of new docs, 0 LOC of code.

## Plan Iteration Log

- v1: initial draft.
- v3: codex impl-review R1 caught 2 BLOCKERs.
  - Comparison capability-table cell for Qualifire claimed
    "Prophet (forecast/trend), all with SHAP" — Prophet
    doesn't use SHAP; SHAP is scoped to shape (Isolation
    Forest) and pattern (RandomForest). Fixed the cell to
    "Isolation Forest (shape) + RandomForest two-sample
    (pattern), both with SHAP; Prophet (forecast / trend)".
  - `docs/comparison.md` had a 7th `## See also` section
    not listed in AC2's strict six. Amended AC2 to allow
    an optional terminal navigation block (consistent
    with the existing `## See also` convention in
    `docs/validators/README.md` and every per-validator
    page under `docs/validators/`).
- v2: codex plan-review R1 caught 3 BLOCKERs.
  - BLOCKER 4: `docs/architecture.md:6-12` opening scope
    block was already stale (claims "Three sections" and
    cites the backfill loop as a backlog item, both
    inaccurate after PR #31 + PR #32). Added an explicit
    Section-D prerequisite step + AC8b requiring the
    update.
  - BLOCKER 6: "Paste into GitHub web editor" mermaid
    gate is not pre-merge verifiable. Replaced with
    AC-Run-3 — `npx -p @mermaid-js/mermaid-cli mmdc`
    render-or-fail on every new diagram before commit.
    Updated Risks table mitigation to match.
  - BLOCKER 7: Tone-calibration had no diff-verifiable
    AC. Added AC13 — `grep -Eni` for a banned-phrase
    set (best documented, industry.leading, vendor
    lock.in, shines, crowded, hit.piece, egress
    required, on.call you can.t escape, best.in.class)
    across the 4 affected files; zero hits required.
