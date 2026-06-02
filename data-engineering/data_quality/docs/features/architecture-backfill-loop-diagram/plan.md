# Plan: architecture-backfill-loop-diagram

## Goal

Add a "Backfill loop" ASCII diagram + section to
`docs/architecture.md` that traces the backfill driver from
operator input (`qualifire backfill --partition START..END`)
through scope resolution, partition expansion, work-list
build, optional ThreadPoolExecutor fan-out, two-pass
per-anchor execution, and the final `BackfillReport`. The
end-to-end value-flow diagram already covers normal
collection; this addition covers the backfill-specific
mechanics that operators trip on most.

## Locked Decisions

1. **One section, 90-140 LOC.** Inserted into
   `docs/architecture.md` immediately after the existing
   "WAP lifecycle" section and before the "What's NOT in this
   doc yet" pointer. Budget covers the new "## 2a. Backfill
   loop" heading through the end of the new prose, not
   counting touched lines in §3.
2. **ASCII diagram, not Mermaid.** Consistent with the rest of
   architecture.md.
3. **Reference concrete module paths** (`qualifire/core/backfill.py:_resolve_scopes`,
   `:_process_anchor`, `:_run_anchor_once`, etc.) so a reader
   can navigate from the diagram to the code. Codex impl
   review on existing diagrams already verifies these paths.
4. **Cover the two-pass per-anchor structure** explicitly —
   that's the part of backfill operators ask about most ("why
   are my tombstones written before the engine runs?").
   Specifically, the section MUST cover:
   - Pass 1a per-metric pre-engine reads (`_read_original_value`
     + `_read_severity_before`) and the rule that a Pass 1a
     failure aborts the entire anchor before any engine work.
   - Pass 1b bulk tombstone write (`build_prior_tombstone_rows`
     + one `storage.write_results` call) ONLY when
     `soft_delete_prior=True`; bulk-write failure aborts the
     anchor cleanly with no partial tombstones landed.
   - Single `_run_anchor_once` call (engine-once contract).
   - Carve-out: when `metric_names` is empty (scope of only
     sample-based validators), engine is skipped entirely and
     the sample-validator block at the end produces "skipped"
     diffs.
   - Sample-validator skipped diffs appended after Pass 2 (and
     also after an engine failure path), with
     `skip_reason="no_historical_samples"`.
5. **Cross-link** to `docs/backfill_and_soft_delete.md` for
   the user-facing operator guide; `docs/CHANGELOG.md` for the
   parallelism / max-partitions follow-ups; and the
   `BackfillReport` shape in `qualifire/core/backfill_report.py`.

## What Changes

### `docs/architecture.md`

- **New section** "2a. Backfill loop" between current §2 (WAP)
  and §3 (What's NOT in this doc yet).
- ASCII diagram with stages:
  1. Operator entry (`qualifire backfill` / `Qualifire.backfill`).
  2. Scope + partition resolution → `_WorkUnit` list (one per
     `(scope, anchor)`; `_WorkUnit.seq_idx` preserves
     serial-mode order so parallel mode can re-sort).
  3. Serial loop OR `ThreadPoolExecutor` fan-out
     (parallelism > 1 forces `notifiers={}` and sets
     `BackfillReport.notifications_suppressed=True`).
  4. Per-anchor `_process_anchor`:
     - Pass 1a: per-metric `_read_original_value` /
       `_read_severity_before`; failure aborts anchor.
     - Pass 1b: bulk tombstone write when
       `soft_delete_prior=True`; failure aborts anchor.
     - Engine-once: `_run_anchor_once` (skipped when
       `metric_names` is empty).
     - Pass 2: per-metric `_extract_metric_severity` →
       `PartitionDiff` per metric; sample validators get
       appended `status="skipped"` diffs at the end.
  5. Sort results by `seq_idx` → `BackfillReport`
     (`partitions` list + `refreshed` / `unchanged` /
     `skipped` / `errored` / `notifications_suppressed`).
- A short prose para covering the recurring questions:
  - Why tombstones write BEFORE the engine (Pass 1b vs the
    single `_run_anchor_once`).
  - Why parallel mode forces `notifiers={}` (suppression
    race).
  - Why per-anchor `_read_original_value` /
    `_read_severity_before` use the anchor partition_ts
    (post `backfill-severity-before-broken-readback`).
  - Why a Pass 1a or Pass 1b failure aborts the entire
    anchor (no partial-state landed; engine.run() skipped;
    every metric on the anchor reports `errored`).
  - Why sample-based validators (Pattern / AnomalyDetection)
    surface as `status="skipped"` and not as per-metric
    diffs.
- **Update §3** ("What's NOT in this doc yet") to remove all
  bullets whose features have already shipped (have a
  `shipped.md`): currently `configuration-reference-audit`,
  `pydantic-docstring-audit`, `docs-lint-ci`, and (once this
  ships) `architecture-backfill-loop-diagram`. Remaining
  bullet after this edit: `configuration-reference-collectors-extension`.

### Backlog cleanup

Once shipped, mark `docs/features/architecture-backfill-loop-diagram/idea.md`
done via a `shipped.md`.

## Acceptance Criteria

### Diff-verifiable (a reviewer can check from the patch alone)

- AC1: New section "## 2a. Backfill loop" exists in
  `docs/architecture.md` after §2 WAP and before §3 pointers.
  Section length 90-140 lines (measured from the heading
  through to the §3 heading, exclusive).
- AC2: The ASCII diagram names concrete module paths that
  exist in the current code: `_resolve_scopes`, `_WorkUnit`,
  `_process_anchor`, `_run_anchor_once`, `BackfillReport`,
  `PartitionDiff`, `_read_original_value`,
  `_read_severity_before`, `build_prior_tombstone_rows`,
  `_extract_metric_severity`.
- AC3: The diagram explicitly labels Pass 1a (pre-engine
  reads), Pass 1b (bulk tombstone — conditional on
  `soft_delete_prior`), engine-once (`_run_anchor_once`),
  and Pass 2 (post-engine diff build).
- AC4: §3 ("What's NOT in this doc yet") no longer lists
  ANY feature that has a `docs/features/*/shipped.md` file.
  After this edit §3 contains exactly one bullet:
  `configuration-reference-collectors-extension`.
- AC5: The prose para explicitly addresses (a) why
  tombstones write BEFORE the engine, (b) why parallel mode
  forces `notifiers={}`, (c) the Pass 1a / Pass 1b
  abort-on-failure semantics, and (d) the sample-validator
  `skipped` path.

### Runtime-gate (verified by running, not by diff)

- AC-Run-1: `pytest tests/test_docs_links.py` still passes
  with no broken-internal-link regressions introduced.
- AC-Run-2: Codex impl review verifies factual accuracy of
  paths + function names vs. the current backfill.py
  (separate from the diff-verifiable ACs above).

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Path or function-name drift from code | AC2 + codex impl review on the spot-grep. |
| Diagram becomes a maintenance burden as backfill evolves | Cross-link to source; future backfill PRs should update the diagram as part of the impl AC (same pattern as the data-flow diagram). |
| ASCII diagram doesn't render well in 80-col viewers | Test rendering during impl (paste into a narrow terminal). |
| §3 cleanup misses a shipped item (or removes a not-yet-shipped item) | AC4 is mechanically checkable: every remaining bullet's feature dir must NOT have a `shipped.md`, every removed bullet's dir MUST have one. |

## Out-of-Band Reviews

- 1 codex plan review (DONE, R1 → 4 BLOCKERs addressed in v2).
- Then implement; 1 codex impl review (factual-accuracy focused).

## Effort

Small. ~100 LOC of new doc content + ~3 lines of §3 cleanup
+ ~5 lines of shipped.md backlog cleanup.

## Plan Iteration Log

- v1: initial draft.
- v2: addressed codex plan-review R1 — split ACs into
  diff-verifiable vs runtime-gate (R1 BLOCKER 1); replaced
  vague "~120 lines" with measured 90-140 LOC budget (R1
  BLOCKER 2); expanded AC4 to remove all shipped backlog
  items from §3, not just this one (R1 BLOCKER 3); added
  explicit coverage requirements for the three
  operator-visible control-flow paths (Pass 1a abort, Pass
  1b conditional + abort, sample-validator skip) to locked
  decision 4 and to AC5 (R1 BLOCKER 4).
