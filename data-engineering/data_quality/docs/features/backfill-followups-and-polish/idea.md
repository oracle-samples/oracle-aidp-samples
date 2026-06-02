---
id: backfill-followups-and-polish
name: Backfill follow-ups + polish (parallelism, auto-detect partition_column, multi-row bulk read, doc/cleanup nits)
type: Enhancement
priority: P2
effort: Medium
impact: Medium
created: 2026-05-09
---

# Backfill Follow-ups and Polish

## Problem Statement

Three genuine future-work items and three low-priority polish items
were deliberately deferred from the
[`backfill-api-and-wap-target-mode`](../backfill-api-and-wap-target-mode/idea.md)
ship in order to keep the parent PR reviewable. They're not
blockers for any operator workflow today — current users get the
full backfill / soft-delete / WAP-rename feature set out of that
PR. This feature collects the deferred items into one backlog
entry so they can be picked up together when the team has cycles.

The items split into two buckets:

### Bucket A — genuine future work (medium effort, real operator value)

1. **Backfill `--parallelism N` flag.** `Qualifire.backfill(...)`
   runs partitions sequentially today. Operators backfilling a
   year of daily partitions (365 anchors × M datasets × K metrics)
   wait through every iteration in series. For history-backed
   validators that don't depend on prior partitions inside the
   same backfill window (most of them), parallelism is safe and
   would compress wall time by Nx. Plumbing question:
   thread-pool vs process-pool vs Spark-job scheduler? Plan picks
   one and pins.

2. **`WAPConfig.partition_column` auto-detection from
   `partition_ts`.** Currently `partition_column` is required for
   target-mode WAP backfill — operators set it explicitly. When
   `dataset.partition_ts` is a bare column reference
   (`event_date`, not a constant Jinja expression like
   `'{{ ds }}'`), we can auto-derive `partition_column = "event_date"`
   without operator effort. Reduces config noise; preserves
   explicit-override.

3. **Multi-row `read_validation_history_bulk(limit > 1)` under
   soft-delete.** The shipped foundation pinned this as
   `ValueError(limit > 1)` because the tombstone-aware multi-row
   walk wasn't designed yet. Today the only consumer is
   notification suppression (limit=1). Future consumers (a
   "show last 3 verdicts per metric" dashboard panel; a
   bulk-export-history CLI tool) need limit > 1. Plan designs
   the cumulative-active-walk and rewrites every backend's
   bulk read.

### Bucket B — polish carried from codex impl-review

4. **Foundation-doc-vs-code drift fix.**
   `docs/features/metrics-backfill-and-soft-delete/shipped.md`
   claims certain scaffolds shipped (`backfill_eligibility.py`,
   `expected_metrics()` resolver,
   `Qualifire.validate(skip_if_cached=...)`,
   `_add_backfill_tag` helper) that did not exist on `main` until
   `backfill-api-and-wap-target-mode` created them. Pure
   docs-only PR fixing the historical record so future
   archaeologists don't get confused.

5. **CLI `qualifire backfill` error routing.** Codex impl-review
   R1 SHOULD-FIX. The summary printer emits per-diff errors in
   the same line stream as successes; a stricter pattern would
   route `errored` lines to stderr and keep stdout for the
   normal report. Cosmetic ergonomics; matters when operators
   pipe `--json` into a downstream tool.

6. **Configurable `expand_partition_ts` cap.** Codex impl-review
   R1 NIT. The 10000-partition cap is a magic number. Could be
   either a `qualifire/core/config.py` constant with a docstring
   pointing to the rationale, or a `Qualifire.backfill(
   max_partitions=...)` kwarg. Operators who legitimately need
   to backfill 50k+ partitions in one shot would otherwise have
   to chunk by hand.

## Why this matters

- **Bucket A** is real operator value but is not blocking
  anything today. Multiple internal teams have asked about
  parallelism in backfill and `partition_column` ergonomics; both
  are reasonable next-quarter pickups.
- **Bucket B** is review-feedback-driven cleanup. None of the
  items are correctness issues — they were carried out of
  the codex impl-review rounds as SHOULD-FIX / NIT items the
  parent PR didn't close. They make the next pickup smoother.

## Affected Areas

- `qualifire/core/backfill.py` — parallelism plumbing,
  configurable cap.
- `qualifire/core/config.py` — `WAPConfig.partition_column`
  auto-detect helper; configurable cap constant.
- `qualifire/storage/{sqlite,delta,external_catalog,jdbc}_storage.py` —
  multi-row `read_validation_history_bulk` rewrite.
- `qualifire/cli.py` — error routing on `_cmd_backfill`.
- `docs/features/metrics-backfill-and-soft-delete/shipped.md` —
  drift fix.
- Tests across all the above.

## Captured Constraints

These reflect decisions already vetted during the parent PR's
review rounds; the plan should treat them as inputs, not open
questions:

1. **Sequential is the safe default.** Parallelism is opt-in via
   `--parallelism N` (or programmatic kwarg). Default stays N=1
   so existing operator behavior is unchanged.
2. **Auto-detection is opt-in via "explicit > implicit".** When
   `wap.partition_column` is set, it wins. When it's omitted AND
   `dataset.partition_ts` is a bare column reference, the
   resolver derives `partition_column` from it. When
   `partition_ts` is a Jinja constant or a SQL expression that
   isn't a column ref, auto-detection bails and the existing
   "missing partition_column" error fires.
3. **Multi-row bulk read keeps `limit=1` as the default.** No
   existing caller wants > 1; the rewrite ships behind
   `limit > 1` opt-in so default behavior stays cheap.

## Open Questions (for the plan phase)

- **Q1.** Parallelism mechanism — thread-pool (simple, GIL-bound
  for pure-Python validators), process-pool (GIL-free but
  expensive), or Spark-job scheduler (forwards Spark jobs into
  the existing scheduler pool)?
- **Q2.** Does `partition_column` auto-detect look at the
  rendered or the unrendered `partition_ts`? Constants like
  `'{{ ds }}'` render to a value, not a column.
- **Q3.** Multi-row bulk read — order-by `run_timestamp DESC`
  is obvious, but do we surface tombstones in the result set
  (with an `is_active='false'` field) or filter them out? The
  notification-suppression caller wants filtered; an audit
  consumer might want unfiltered.

## References

- Parent feature:
  [`backfill-api-and-wap-target-mode`](../backfill-api-and-wap-target-mode/idea.md)
  — full A-M scope shipped 2026-05-09 in PR #13.
- Foundation:
  [`metrics-backfill-and-soft-delete`](../metrics-backfill-and-soft-delete/idea.md).
- Codex impl-review rounds R1-R5 transcripts (in the parent PR's
  shipped.md).

## Notes

- Items 1-3 (Bucket A) are independent — each could be a
  standalone follow-up PR if the team wants to phase them.
  Bundling here keeps them on one backlog card rather than three.
- Items 4-6 (Bucket B) are docs / cosmetic and could ride along
  any of the Bucket A PRs as a small commit, or land as a
  separate cleanup PR.
