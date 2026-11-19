---
description: Summarize the reports from the most recent migrate/review/reconcile/deploy/optimize/lineage runs found in the working directory. There is no single JOB_REPORT.md in this tool — this command finds and reads whichever report directories exist.
---

# `/infa-status` — summarize the last run's reports

Unlike some sibling migrator plugins, this tool has no single consolidated
run report. Each command writes its own report to its own output
directory (default names below, all overridable with `-o`). This command
finds whichever of those exist under the given (or current) directory and
summarizes them together.

## Workflow

1. Resolve a base directory from `$ARGUMENTS`, else use the current
   directory.
2. Look for, in this order, and read whichever are present:
   - `analysis_report/` — from [`infa-analyze`](../skills/infa-analyze/SKILL.md)
   - `<migrate-output>/reports/fidelity_report.md` and `reports/confidence_report.md` — from `infa-migrate-mapping` (always written)
   - `<migrate-output>/comparisons/` — from `infa-migrate-mapping --comparison`
   - `<migrate-output>/workflows/*.review.md` — workflows that did not translate completely
   - `review/review_report.md` — from [`infa-review`](../skills/infa-review/SKILL.md)
   - `reconcile_report.md` (or `./reconcile_report/reconcile_report.md`) — from [`infa-reconcile`](../skills/infa-reconcile/SKILL.md)
   - `deploy_report/deploy_report.md` — from [`infa-deploy`](../skills/infa-deploy/SKILL.md)
   - `optimization_report/optimization_report.md` — from [`infa-optimize`](../skills/infa-optimize/SKILL.md)
   - `lineage/` — from [`infa-lineage`](../skills/infa-lineage/SKILL.md)
3. For anything not found, say so explicitly rather than omitting it
   silently — "no reconcile report found" is itself useful information
   (it tells the user reconciliation hasn't happened yet).
4. Recommend the next command based on what's missing, using the pipeline
   order from [`infa-migrator-overview`](../skills/infa-migrator-overview/SKILL.md):
   analyze → migrate → review → deploy → (run the job in AIDP) → reconcile.

## Args

`$ARGUMENTS`

A directory to search, else the current directory.

## Output template

```
== infa2aidp status: <base-dir> ==

analyze     : found  — analysis_report/ (N mappings, E errors)
migrate     : found  — 23 notebook(s), fidelity gaps in 4
review      : found  — 20 approved, 2 edited, 1 rejected
reconcile   : NOT FOUND — no reconcile run yet
deploy      : NOT FOUND

RECOMMENDATION: run /infa-reconcile once the migrated notebooks have
actually been run against a live AIDP target — this tool does not execute
them for you.
```

## Notes

- This command only reads report files already on disk; it never
  re-invokes `discover`/`analyze`/`migrate`/etc itself.
- If multiple runs' worth of reports are in the directory (e.g. re-run
  `infa-migrate-mapping` with different flags), file modification times
  are the only signal for "most recent" — say so if it's ambiguous rather
  than guessing which is authoritative.
