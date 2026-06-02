---
id: architecture-backfill-loop-diagram
name: Architecture — Backfill Loop ASCII Diagram
type: Documentation
priority: P3
effort: Small
impact: Low
created: 2026-05-10
---

# Architecture — Backfill Loop ASCII Diagram

## Problem Statement

`docs/architecture.md` (shipped in
`comprehensive-config-and-architecture-docs`) covers the
end-to-end value-flow diagram and the WAP lifecycle, but does
NOT include a backfill driver loop diagram. The backfill flow
has its own non-trivial mechanics: scope resolution → partition
expansion → per-anchor work-units → optional parallelism →
two-pass per-anchor (read-orig-value → tombstone-write →
engine.run() once → diff). Operators reading the architecture
doc can't see this without diving into `qualifire/core/backfill.py`.

## Scope

Add a "Backfill loop" section to `docs/architecture.md`:

- ASCII diagram showing scope resolution → expansion →
  per-anchor `(scope, anchor)` units → ThreadPoolExecutor
  fanout → `_process_anchor`'s two-pass structure →
  `BackfillReport`.
- Cross-link to `docs/backfill_and_soft_delete.md` for the
  user-facing operator guide.
- 30-50 LOC.

## Why It Matters

- Backfill is the most-asked-about feature in operator
  conversations. The data flow + WAP diagrams answer "how
  does normal collection work?" but not "how does a 90-day
  replay actually drive the engine?"

## Why Deferred

Captured during `comprehensive-config-and-architecture-docs`
v2 scope cut. The data flow + WAP diagrams cover the most
common operator questions; the backfill loop is a P3 follow-
up.

## Affected Areas

- `docs/architecture.md`.
