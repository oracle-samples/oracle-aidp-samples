---
description: Parse + summarize the JOB_REPORT.md produced by a prior migration run. Surfaces cell pass/fail/fix counts per task, lists the failing cells, and recommends a next-step skill (fixup-cell, resume, or all-clear).
---

# `/migration-status` — parse the last migration report

Reads `JOB_REPORT.md` (the per-cell pass/fail summary the migrator emits) and converts it to a quick verdict + per-task action list.

## Workflow

1. Locate `JOB_REPORT.md`. Default to the most recent file at `reports/<job>/JOB_REPORT.md` (local mirror) or `<output-base>/<job>/JOB_REPORT.md` (workspace path — fetch via REST if no local mirror).
2. Parse the per-task table.
3. Roll up totals + identify the failing cells.
4. Recommend the next skill to invoke per failing-task.

## Args

$ARGUMENTS

If `$ARGUMENTS` is a path or a job name, target that. If empty, locate the newest.

## Output template

```
== Migration status: <MyJob> (run dated 2026-XX-XX HH:MM:SS UTC) ==

| Task            | Status   | Cells: OK | Fail | Fixed | Notes |
|---|---|---|---|---|---|
| <task_a>        | PASS     | 27        | 0    | 2     |  |
| <task_b>        | PARTIAL  | 18        | 2    | 7     | cell 12, cell 19 |
| <task_c>        | FAIL     | 0         | -    | -     | dep notebook unresolved |

Overall: 2/3 PASS, 1 PARTIAL, 0 FAIL → RESULT: PARTIAL

Recommended actions:
  • <task_b>: cells 12 + 19 failed all 10 attempts.
    → Invoke aidp-fixup-cell on each with a `why` reason.
  • <task_c>: structural failure (dep `helpers/io_utils` couldn't be migrated).
    → Open dep .ipynb manually, fix, then aidp-resume-migration with --only-tasks <task_c>.

Acceptance contracts: (if applicable)
  • <task_a>: PASS — 3 consecutive zero windows observed.
```

## Reading the underlying file

Format is documented in [references/job-report-format.md](../references/job-report-format.md). The key fields:

- Per-task `Status` ∈ {`PASS`, `PARTIAL`, `FAIL`}.
- Cell counts: `OK` (executed clean), `Failed` (exhausted 10 attempts), `Fixed` (passed after >=1 retry).
- Failing cell IDs listed in the "Errors & Warnings" section.

## When to use

- After any [`aidp-migrate-job`](../skills/aidp-migrate-job/SKILL.md) run.
- When the user asks "how did the migration go", "did it pass", "what failed".
- Before deciding which fix path to take.

## Routing

| Verdict | Next |
|---|---|
| PASS, no acceptance contract | Done. Acknowledge + sign off. |
| PASS, with PASS contracts | Done. Confirm contracts converged. |
| PARTIAL with N failing cells | Route to [`aidp-fixup-cell`](../skills/aidp-fixup-cell/SKILL.md) for each. |
| FAIL on dep migration | Route to manual fix → [`aidp-resume-migration`](../skills/aidp-resume-migration/SKILL.md). |
| ACCEPTANCE_CONTRACT_VIOLATED | Investigate why the pending queue didn't drain. See [`aidp-acceptance-contract`](../skills/aidp-acceptance-contract/SKILL.md). |
