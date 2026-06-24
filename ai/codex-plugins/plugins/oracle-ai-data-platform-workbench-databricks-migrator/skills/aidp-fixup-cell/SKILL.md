---
name: aidp-fixup-cell
description: Targeted rewind of a migrated notebook. Re-executes cells from history index N onwards (or a specific cell range) through the execute+verify+fix loop, with a 'why' reason injected so Claude knows what to fix. Use when aidp-migrate-job left a notebook at RESULT=PARTIAL or the user identifies a specific cell that is wrong post-migration.
---

# `aidp-fixup-cell` — surgical re-execute of cells in a migrated notebook

The full-job migrator (`job_migrate.py`) runs every cell linearly with up to 10 fix attempts each. `fixup_cell` is the per-cell escape hatch when those 10 attempts weren't enough OR when the user discovers a latent issue downstream.

## When to use

- `JOB_REPORT.md` shows specific cells as `FAIL` after [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) finished.
- User points at a specific cell of a previously-migrated notebook and says "this is wrong, fix it".
- User has manually edited cell K to set up a precondition, wants K+1..K+N to re-run accounting for the change.

## Two invocation modes

### Mode A — re-run from a history index (Opus tool)

Inside an already-running migration, Claude can call the `fixup_cell` tool:

```
fixup_cell(start_index=12, why="cell 11 redefined `Basedata` to use a different schema; replay downstream so the new var flows through")
```

This is the in-process mode. It:
1. Truncates `_cell_history[start_index:]` (drops everything from index 12 forward).
2. Replays each old entry through `_replay_cell_entry()` — execute + verify + fix loop with the `why` injected into the Claude prompt so the model knows what changed.
3. Appends the new (post-replay) entries back to `_cell_history`.

The cells replayed start at the absolute history index 12 — could be in the SAME notebook or a downstream one if the cells were inlined via `%run`.

### Mode B — standalone replay against a saved notebook

When the migration is done and the user wants to "fix this one cell":

```bash
python3 scripts/job_migrate.py \
  --manifest reports/<MyJob>_manifest.json \
  --cluster <CLUSTER_ID> \
  --only-tasks <task_key> \
  --no-skip-migrated
```

This re-runs that single task end-to-end (deps already cached → fast). Combine with `--start-task <substring>` to run only that one task.

If the user wants finer-grained control (specific cell, not full task), they need to open the saved `.ipynb` and edit manually. The migrator doesn't expose a "replay cell K only" CLI outside of the in-process fixup_cell tool.

## When fixup_cell will help — and when it won't

**Helps:**
- The cell's failure is due to upstream state the migrator's auto-fix didn't anticipate (variable shape, schema drift, missing import).
- The cell needs context Claude didn't have in the first pass (a hidden dependency, a manual override the user just applied).
- The notebook flow needs to be replayed after a structural fix (e.g. you redefined a function).

**Won't help:**
- The cluster itself is misconfigured (missing JAR, wrong Spark version) — fix the cluster, not the cell.
- The source data is missing / wrong shape — fix the data, not the cell.
- The migrator is generating a known-bad construct (e.g. a `dbutils` call that should have been rewritten) — check [`references/gotchas.md`](../../references/gotchas.md) for the recipe, then re-run with the fix.

## Important: idempotency requirement

`fixup_cell` replays cells from `start_index` FORWARD. This is **only safe if the replayed cells are idempotent**. If a cell:
- writes to a sandbox table with `.mode("overwrite")` → safe.
- writes to a sandbox table with `.mode("append")` → unsafe, will duplicate rows on replay.
- mutates external state (REST API call, write to OCI Object Storage) → unsafe.

Before triggering a replay, scan the cells from `start_index` for non-idempotent ops. If you find any, fix THEM first, then replay.

## How the `why` reason flows

The `why` string is injected into the Claude prompt as:

```
=== CONTEXT: WHY WE'RE REPLAYING ===
The previous run hit a problem at this stage. Reason: <your why>

Replay each cell with this context in mind. If the prior code was correct
and only the upstream state changed, you can keep it as-is. If the prior
code needs adjustment to handle the new state, rewrite as needed.
```

So make the `why` precise — vague reasons produce vague fixes:

- ✅ Good: `"cell 11 was rewritten to use spark.read.table('<sandbox_schema>.events') instead of spark.read.parquet('s3://...'); downstream cells reference the same path and need similar adjustment"`
- ❌ Bad: `"something broke, please retry"`

## Spotting a failure that NEEDS fixup_cell vs one that doesn't

| Symptom | Action |
|---|---|
| Cell K failed 10 attempts, all with the same error | Try fixup_cell with a specific `why` that names the error. |
| Cell K failed because cell K-2 produced a different schema than expected | fixup_cell from K-2 (not K) with `why` describing the upstream change. |
| Cell K failed because of a known gotcha (see [references/gotchas.md](../../references/gotchas.md)) | Apply the gotcha-recipe fix in cell K manually, then fixup_cell from K with `why="applied gotcha #N fix"`. |
| Cell K failed because the cluster died mid-execution | Restart the cluster, re-invoke [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) with `--skip-migrated` — fixup not needed. |

## After this

- Re-read `JOB_REPORT.md` to confirm the cell is now `OK` ([`/migration-status`](../migration-status/SKILL.md)).
- If the replay produced more failures, that's a structural problem — escalate to [`migration-reviewer`](../migration-reviewer/SKILL.md) for a second-opinion review.
