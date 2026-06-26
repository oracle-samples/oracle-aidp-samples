---
name: migrate-job-flow
description: Guided full-job migration flow. Walks the user from "I want to migrate this Databricks job" through DAG → data check → migrate → status, asking before each phase. Composes aidp-build-dag, aidp-check-data, aidp-migrate-job, and migration-status. Use when the user wants the end-to-end guided experience instead of running individual phases manually.
---

# `migrate-job-flow` — guided full migration

Drive the migrator end-to-end with checkpoints between phases. Asks the user before running each long-running step so they can abort or adjust.

## When to use

- User says "migrate this Databricks job", "port this workflow end-to-end", "run the full migration".
- User wants a single guided flow, not individual phase invocations.
- User wants checkpoints between phases (instead of fire-and-forget).

For individual phases, prefer:
- DAG build only → [`aidp-build-dag`](../aidp-build-dag/SKILL.md)
- Data check only → [`aidp-check-data`](../aidp-check-data/SKILL.md)
- Migrate only (manifest already exists) → [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md)

## Workflow

1. **Confirm prerequisites** — invoke [`aidp-migrator-bootstrap`](../aidp-migrator-bootstrap/SKILL.md) silently; surface any gaps.
2. **Ask for the source** — either a Databricks workspace path OR a Databricks Job ID. Don't proceed without it.
3. **Build the DAG** — invoke [`aidp-build-dag`](../aidp-build-dag/SKILL.md). Show the resulting task count + dep count. Ask "looks right?" before next step.
4. **Data check** — invoke [`aidp-check-data`](../aidp-check-data/SKILL.md). Show the OK / MISSING / EMPTY counts.
   - If any MISSING / EMPTY: ask the user how to handle. Options: (a) migrate catalog first via [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md), (b) configure [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md), (c) proceed anyway (with a clear warning).
5. **Migrate** — invoke [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md). Surface live log tail.
6. **Status** — when the run finishes, invoke [`migration-status`](../migration-status/SKILL.md) on the resulting `JOB_REPORT.md`.

## Args

If the user supplied a Databricks Job ID or workspace path in their prompt, jump straight to step 3 (skip the question in step 2).

## Checkpoints to surface to the user

Between phases, give the user a 1-line summary + the cost estimate of the next step:

```
[Phase 1/5] DAG built: 7 tasks, 18 dep notebooks, output reports/<MyJob>_manifest.json
[Phase 2/5] About to scan source data on cluster — ~2 min, no token cost. Proceed? (y/N)
...
[Phase 4/5] About to start Pass-2 migration — est. 30-90 min, token cost depends on the selected OpenAI model. Proceed? (y/N)
```

Do not auto-proceed past the migrate step without explicit user confirmation. Pass-2 is expensive.

## When to stop

- User aborts at any checkpoint.
- Data check shows >50% MISSING — likely the catalog hasn't been migrated yet; route to [`migrate-catalog-flow`](../migrate-catalog-flow/SKILL.md) instead.
- Cluster is not Active — instruct the user to start it before continuing.
