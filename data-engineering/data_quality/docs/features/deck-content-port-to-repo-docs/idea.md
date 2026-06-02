---
id: deck-content-port-to-repo-docs
name: Port Deck-Draft Content Into Repo Docs (Comparison + Visual Validator Chooser + Architecture Diagrams)
type: Enhancement
priority: P2
effort: Medium
impact: Medium
created: 2026-05-11
---

# Port Deck-Draft Content Into Repo Docs

## Problem Statement

A Qualifire pitch-deck draft lives outside the repo at
`~/OracleContent/idl/data-quality/qualifire-overview/README.md`. The
draft was assembled by reading the live codebase + `docs/`, so its
content tracks reality, but the reverse direction is incomplete:
several pieces that prospective AIDP users would benefit from on the
public docs surface exist *only* in the deck draft.

Specifically, the repo's docs are strong on **reference** (per-validator
pages, the new `docs/configuration.md` Pydantic reference, the new
`docs/architecture.md` data-flow + WAP lifecycle + backfill loop)
but thin on **evaluation-grade material** that helps someone deciding
whether to adopt Qualifire:

1. **No "Why Qualifire" framing on the README.** It jumps straight
   into Key Features + install. An evaluator landing from a search
   result or a deck has no 30-second answer to "what does this
   library solve that I can't get from Great Expectations / Deequ /
   Soda / dbt tests?"
2. **No comparison page.** Anyone who has worked in DQ for more
   than a year will ask "how is this different from GE?". Today the
   answer requires reading the README, the validator pages, and the
   architecture doc to triangulate. The deck draft has a worked
   side-by-side that just doesn't live in the repo.
3. **Validator chooser is text-only.** `docs/validators/README.md`
   has one table ("how to choose") with one row per validator. The
   deck draft adds a visual complexity ladder, a decision
   flowchart, and three concrete scenarios with tradeoffs — all of
   which were written by reading the actual validator code, so they
   describe today's behaviour accurately.
4. **Architecture doc has no visual class-of-component decomposition.**
   `docs/architecture.md` traces one value end-to-end (great for
   "how does it work?") but never zooms out to "what are the four
   layers and which class lives in which?". The deck draft has a
   four-layer mermaid + an engine-pipeline sequence diagram that
   complement the existing ASCII rather than replacing it.

The two stateless-vs-stateful labels are used throughout the
validator pages and `docs/configuration.md` but never *defined* in
one place. The deck draft frames them as a read-pattern split,
which is the cleanest framing and would slot naturally into
`docs/architecture.md`.

## Why It Matters

- **Evaluator UX.** Today the "is this the right tool for me?"
  question takes ~30 minutes of doc-reading. With (A) + (B), it
  takes 5.
- **Single source of truth.** The deck draft is unversioned and
  lives outside the repo. Putting the evaluation-grade content in
  the repo means the next refactor / rename / feature ship updates
  the doc as a normal PR review item, not as an afterthought.
- **Internal sales.** The comparison page becomes the link AIDP
  account engineers can send when a customer asks "why not Monte
  Carlo?". Today they'd send a Slack screenshot of the deck.

## Who Benefits

- **Prospective AIDP customers and internal AEs** evaluating
  Qualifire against GE / Deequ / Soda / dbt-tests / Pandera /
  SaaS-DQ-vendors.
- **First-time contributors** who want a visual mental model of the
  four-layer architecture before reading `engine.py`.
- **Operators triaging a drift / shape / pattern alert** who need
  a decision tree for "which validator should I be running here?".

## Scope (confirmed with operator)

### A — README "Why Qualifire" section
- Insert a new section at the top of `README.md`, **before** the
  existing Key Features list.
- Content: 3–4 sentence motivation prose ("modern data platforms
  ship in hours, the failure mode is silent corruption, here's why
  hand-rolled asserts / SaaS DQ vendors / per-team conventions
  leave a gap") + the impact-on-users persona table (Data engineer
  / Pipeline owner / On-call / Compliance / Platform team —
  before / with Qualifire).
- ~25–40 lines net add.

### B — New `docs/comparison.md`
- Port the deck draft's §12 verbatim-ish.
- Include the mermaid landscape diagram (in-compute libraries vs
  SaaS-hosted DQ).
- Include the side-by-side capability table (Runtime / History
  store / ML / WAP / Notifications / AIDP fit) covering:
  Qualifire, Great Expectations, Deequ / PyDeequ, Soda Core OSS,
  dbt tests + dbt-expectations, Pandera, Monte Carlo / Bigeye /
  Anomalo.
- Per-library "where it shines vs gets in the way" prose.
- Explicit "what Qualifire is NOT trying to be" (schema enforcement,
  lineage / catalog, SaaS observability product).
- One-line positioning statement at the bottom.
- Add a "See also" link to the new page from the README and from
  `docs/validators/README.md`.

### C — Enhance `docs/validators/README.md`
- Keep the existing "How to choose" chooser table — it's
  still the right reference.
- Add **above** the table:
  - Mermaid complexity ladder: SLO → threshold → drift →
    forecast → shape → pattern, with stateless / stateful
    annotation.
- Add **below** the table:
  - Mermaid decision flowchart for the
    "drift vs shape vs pattern" pick.
  - Three concrete scenarios from the deck draft (revenue ratio
    drop → drift wins; encoding migration → shape wins;
    correlated-columns ETL bug → pattern wins) each with a one-line
    tradeoff resolution.

### D — Enhance `docs/architecture.md`
- Add a mermaid four-layer decomposition (Engine + Collection /
  Validation / Notification / SystemTable) early in the doc, with
  the existing ASCII traces kept underneath as the
  "follow-one-value-through" detail.
- Add a mermaid sequence diagram for one `qf.validate(...)` call
  showing the User → Engine → Backend → Validator → SystemTable →
  Notifier interactions.
- Add a new short section: **Stateless vs stateful validators**.
  Defines the split, names which validators land on which side,
  the `partition_ts` requirement for stateful, the cold-start
  contract (`on_missing_history`), and how the split drives the
  `qf.backfill(...)` API.

## Non-goals

- **No duplication of detail already in repo docs.** Per-validator
  detail stays in `docs/validators/*.md`. WAP detail stays in
  `docs/wap_pattern.md`. Skip flags / redaction / backfill detail
  stay in `docs/configuration.md` + `docs/backfill_and_soft_delete.md`.
- **No roadmap content in any of these files.**
  `docs/features/DASHBOARD.md` is the canonical roadmap.
- **No new validator / collector / notifier code.** This is a
  pure-docs port.
- **No changes to the deck-draft file under `~/OracleContent/`** —
  that file stays as the unversioned pitch artefact.

## Affected Areas

- `README.md` (Section A)
- `docs/comparison.md` (new — Section B)
- `docs/validators/README.md` (Section C)
- `docs/architecture.md` (Section D)
- `docs/CHANGELOG.md` — Enhancement entry naming the four
  surfaces.

## Open Questions for Planning

- **Comparison-page tone calibration.** The deck draft uses
  direct prose ("Great Expectations is the best documented DQ
  library and has the biggest expectation catalogue. The cost:
  Data Docs, the Datasource concept, the Action runner …"). Is
  that tone right for a repo doc, or should we sand it down to
  more neutral language? Plan phase decides.
- **Should `docs/comparison.md` live under `docs/` directly, or
  under a new `docs/marketing/` or `docs/evaluation/` subfolder?**
  Plan phase decides; the deck-draft port is small enough that
  the placement doesn't constrain future content.
- **Mermaid in `docs/validators/README.md` —** GitHub renders it
  natively; the README index page is one of the more frequently
  hit pages. Are there rendering surfaces (e.g. the PyPI long-
  description, internal Confluence mirror) where mermaid will
  fall back to literal code-fences? If yes, plan phase considers
  a static SVG fallback or keeps the chooser table as the
  text-only canonical chooser with the diagram as enrichment.
- **README size budget.** README.md is already substantial. The
  "Why Qualifire" insert is ~30 lines; is there a precondition
  cut elsewhere in the README (e.g. fold the install matrix into
  `docs/installation.md`) that should ride with this? Probably
  not — let plan phase confirm.
