---
description: Parse + summarize the JOB_REPORT.md produced by a prior migration run. Surfaces cell pass/fail/fix counts per task, lists the failing cells, and recommends a next-step skill (fixup-cell, resume, or all-clear).
---

# `/migration-status` — parse the last migration report

Reads `JOB_REPORT.md` (the per-cell pass/fail summary the migrator emits) and converts it to a quick verdict + per-task action list.

## Workflow

1. Locate `JOB_REPORT.md`. Default to the most recent file at `reports/<job>/JOB_REPORT.md` (local mirror) or `<migrate-output>/<job>/JOB_REPORT.md` (workspace path — fetch via REST if no local mirror).
2. Parse the per-task table.
3. Roll up totals + identify the failing cells.
4. Recommend the next skill to invoke per failing-task.

## Args

$ARGUMENTS

If `$ARGUMENTS` is a path or a job name, target that. If empty, locate the newest.

## Output template

```
== Migration status: <MyJob> (run dated 2026-XX-XX HH:MM:SS UTC) ==

| Task     | Status                   | OK | Failed | DataUnavail | Fixed | Tokens  | Notes |
|---|---|---|---|---|---|---|---|
| <task_a> | PASS                     | 27 | 0      | 0           | 2     | 41,210  |  |
| <task_b> | PARTIAL                  | 18 | 2      | 0           | 7     | 120,004 | cell 12, cell 19 |
| <task_c> | PARTIAL_DATA_UNAVAILABLE | 12 | 0      | 3           | 1     | 33,816  | 3 cells blocked on missing tables |
| <task_d> | FAIL                     | 0  | -      | -           | -     | 0       | dep notebook unresolved |

Overall: 1/4 PASS, 2 PARTIAL-family, 1 FAIL → RESULT: PARTIAL
Claude tokens (billed: uncached input + cache reads + cache writes + output): 195,030 total

Recommended actions:
  • <task_b>: cells 12 + 19 failed all 5 attempts.
    → Invoke aidp-fixup-cell on each with a `why` reason.
  • <task_c>: 3 cells blocked on genuinely missing data.
    → Fix the data (backfill / /migrate-catalog / bucket mapping), then re-run the task.
  • <task_d>: structural failure (dep `helpers/io_utils` couldn't be migrated).
    → Open dep .ipynb manually, fix, then aidp-resume-migration with --only-tasks <task_d>.

Acceptance contracts: (if applicable)
  • <task_a>: PASS — 3 consecutive zero windows observed.
```

## Reading the underlying file

Format is documented in [references/job-report-format.md](../references/job-report-format.md). The key fields:

- Per-task table columns: `| Task | Status | OK | Failed | DataUnavail | Fixed | Tokens |`.
- Per-task `Status` vocabulary: `PASS`, `PASS_CONDITIONAL_EXIT_COVERAGE`, `PASS_DATA_SUBSTITUTED`, `PARTIAL`, `PARTIAL_DATA_UNAVAILABLE`, `PARTIAL_PARAM_MISSING`, `PARTIAL_STATE_LOST`, `FAIL`, `BLOCKED`, `ALREADY_MIGRATED`, `ACCEPTANCE_CONTRACT_VIOLATED`.
- Cell counts: `OK` (executed clean), `Failed` (exhausted 5 attempts), `DataUnavail` (blocked on genuinely missing data), `Fixed` (passed after >=1 retry); `Tokens` = the task's billed Claude tokens.
- Token total line: `Total tokens across all tasks: <n> (billed: uncached input + cache reads + cache writes + output).`
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
| PASS_CONDITIONAL_EXIT_COVERAGE | Success (conditional `notebook.exit()` fired; cells below it converted code-only). Validate the other branch by re-running when the condition flips. |
| PASS_DATA_SUBSTITUTED | Success, but review the documented data substitutions before sign-off. |
| PARTIAL with N failing cells | Route to [`aidp-fixup-cell`](../skills/aidp-fixup-cell/SKILL.md) for each. |
| PARTIAL_DATA_UNAVAILABLE | Data gap, not a code failure — fix the data ([`/check-data`](./check-data.md), [`/migrate-catalog`](./migrate-catalog.md), bucket mapping), then re-run the task. |
| PARTIAL_PARAM_MISSING | Supply the missing parameter (`--param KEY=VALUE` / manifest) and re-run; dependents were BLOCKED. |
| PARTIAL_STATE_LOST | Kernel/session died mid-task — restart the cluster, re-run the task. |
| FAIL on dep migration | Route to manual fix → [`aidp-resume-migration`](../skills/aidp-resume-migration/SKILL.md). |
| ACCEPTANCE_CONTRACT_VIOLATED | Investigate why the pending queue didn't drain. See [`aidp-acceptance-contract`](../skills/aidp-acceptance-contract/SKILL.md). |
