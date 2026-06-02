---
id: comprehensive-config-and-architecture-docs
name: Comprehensive Configuration & Architecture Documentation
type: Enhancement
priority: P1
effort: Medium
impact: High
created: 2026-05-06
---

# Comprehensive Configuration & Architecture Documentation

## Problem Statement

The library has accumulated a substantial number of configuration
knobs across validators, collectors, datasets, system-table backends,
notifications, WAP, partition tracking, and (soon) backfill +
soft-delete. Today, those knobs live in three places and nowhere is
fully authoritative:

1. **Pydantic config classes** in `qualifire/core/config.py` carry
   the truth (types, defaults, validators) but the docstrings are
   tuned for engineering review, not for the data engineer asking
   "what does `compare.step` do and what does it default to?"
2. **README.md** has a quick-start, an installation matrix, and a
   handful of inline examples. It does not enumerate every option
   for every validator, doesn't explain how `partition_ts` works
   end-to-end, and predates several recent additions
   (signed drift, rate-of-change, descriptions, soft-delete plans,
   interactive dashboard).
3. **Architecture coverage** is sparse. There's no single
   "operator's guide" that walks through how a metric flows from
   an aggregation expression → CollectionResult → system-table row
   → a downstream history-backed validator's read. The
   notebooks demonstrate it but reading 40 notebook cells isn't
   how someone evaluates whether qualifire fits their stack.

Concrete pain points that surface:

- A team adopting qualifire reads the README, copies the YAML
  example, runs into `partition_ts` / `partition_step` /
  `compare.step` for the first time and has to git-grep the source
  to learn what's required vs. optional.
- The new `{min, max}` signed threshold form, `rate_of_change_pct`
  measure, and `description` fields are documented only via
  in-code docstrings and a few new tests; nothing in the README
  introduces them at the right narrative level.
- The forthcoming **metrics-backfill-and-soft-delete** feature
  introduces *more* knobs (`include`/`exclude` selectors,
  `partition_step` mandatory rule, `is_active` semantics, WAP
  publish-phase contract, `qualifire backfill` + `qualifire
  deactivate-metric` CLIs). Without a documentation refresh first,
  shipping that feature would compound the same problem.
- Operators on AIDP need to wire job parameters through the Jinja
  bridge (`{{ job.* }}`), pick a system-table backend
  (`external_catalog` / `jdbc` / `delta`), and reason about
  driver-only deployment. The README touches these but doesn't
  pull them into a single deployment narrative.

## Proposed Solution (high-level)

Three deliverables, treated as one feature because they're the
same audience read in sequence:

1. **A configuration reference** that catalogues every config
   surface (every Pydantic class, every CLI flag, every API
   kwarg) with: type, default, accepted forms, what it does, when
   to use it, common pitfalls, a tiny worked example. Organised
   by surface (Dataset, Validator type, Collector type,
   Notifications, System table, WAP, Backfill, Soft-delete, Engine).
2. **A revised architecture note** that traces a single value
   end-to-end through the system: an aggregation expression →
   collector → CollectionResult → engine row builder →
   storage write → history read → validator → ValidationResult →
   notification → dashboard. Diagrams (ASCII / mermaid) for the
   data flow + the WAP lifecycle + the backfill loop.
3. **A README refresh** that absorbs the parts of the new docs
   that belong on the front page: an honest inventory of what
   the library does today (signed drift, rate-of-change,
   partition tracking, descriptions, dashboards, backfill, WAP),
   the install matrix kept current, a single self-contained Quick
   Start that maps to the reorganised reference + architecture
   docs, and a "best practices" section covering the patterns
   the notebooks demonstrate but the README doesn't yet name.

The split is operator-facing on top, reference-grade in the
middle, narrative-grade at the bottom. Anyone landing on the repo
should be able to: read the README in 5 minutes and know what
qualifire does; click through to the architecture doc in 15
minutes and understand the data flow; click through to the config
reference whenever they hit a specific knob and find its
authoritative answer in one place.

## Affected Areas

- `README.md` — refresh + grow ~30%; absorb signed-drift, RoC,
  descriptions, partition tracking, dashboards, backfill into
  Key Features and Quick Start.
- `docs/architecture.md` — new file (or revive existing one) —
  end-to-end data flow + WAP lifecycle + backfill loop diagrams +
  storage read pattern (latest-row-per-key + is_active filter
  ordering once that lands).
- `docs/configuration.md` — new file — the comprehensive
  reference above. Organised by surface; cross-links to API and
  CLI surfaces.
- `docs/best-practices.md` — new file or absorbed — patterns:
  per-partition flow with helpers, history-backed validator
  warm-up, dashboard cadence, system-table sizing, WAP for
  partial-day backfills.
- Pydantic class docstrings under `qualifire/core/config.py` —
  audit + tighten so they read as primary source for the
  configuration reference (the reference can quote them).

## Context

- Should land **before** the metrics-backfill-and-soft-delete
  feature ships so backfill + soft-delete documentation slots into
  an existing reference rather than getting bolted onto a
  README that doesn't have a place for it.
- Recently-shipped capabilities (partition tracking, signed
  drift, rate-of-change, descriptions, interactive dashboard) all
  need their first proper documentation pass; this is the
  cleanest moment to do it before more accumulates.
- The notebooks under `tests/manual/` are excellent learning
  artefacts but operators currently treat them as the docs of
  record. They should reference the new docs rather than be
  the docs.
