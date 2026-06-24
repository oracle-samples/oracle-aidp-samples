---
name: aidp-migrate-job
description: Run the full Databricks→AIDP migration against a manifest. Pass-1 walks the %run dep tree and rewrites Databricks APIs in each dep notebook code-only. Pass-2 executes each task cell-by-cell on a live AIDP cluster, runs 4-way verify (exec error / stderr patterns / Spark logs / Opus eval), and re-attempts up to 10 times via Claude with tool use. Use when the user is ready to actually port the workload (not just plan it). Long-running — typical job takes 10–60 minutes per task depending on cell count.
---

# `aidp-migrate-job` — execute the migration

This is the main event. Pass-1 fixes the code, Pass-2 proves it runs.

## When to use

- The user is ready to migrate (manifest built, data-check clean, catalog migrated, cluster Active).
- The user explicitly asks "migrate", "run the port", "execute the migration".

**Do NOT invoke this skill** without:
- A valid manifest at `reports/<job>_manifest.json` (use [`aidp-build-dag`](../aidp-build-dag/SKILL.md)).
- A clean [`aidp-check-data`](../aidp-check-data/SKILL.md) (or an explicit "I know data is missing, proceed anyway" from the user).
- An ACTIVE AIDP cluster.
- `ANTHROPIC_API_KEY` set.
- `~/.oci/config` valid for the chosen profile.

## Canonical invocation

```bash
export ANTHROPIC_API_KEY=sk-ant-...
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate.py \
  --manifest reports/<MyJob>_manifest.json \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --output-base <output-workspace-path> \
  --oci-profile <profile>
```

For the workflow-shape variant (preserves the Databricks Job task DAG):
```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate_from_workflow.py \
  --manifest reports/<MyJob>_manifest.json \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --output-base <output-workspace-path> \
  --oci-profile <profile>
```

Tail the log in another terminal:
```bash
tail -f /tmp/migration.log
```

## Useful flags

| Flag | When to use |
|---|---|
| `--jobs <name>,<name>` | Migrate only specific jobs from the manifest. |
| `--start-task <substring>` | Resume from this task (skip everything before). Pairs with [`aidp-resume-migration`](../aidp-resume-migration/SKILL.md). |
| `--only-tasks <names>` | Run ONLY these specific tasks. Useful for re-running a single failed task. |
| `--skip-migrated` | Skip notebooks already migrated in a prior run (default ON). Set off with `--no-skip-migrated`. |
| `--parallel <N>` | Concurrent task workers (default 20). Reduce if the cluster is contended. |
| `--catalog-manifest <path>` | Apply deterministic source-catalog → `default` remap in string literals. Required when source code has hardcoded `<source-catalog>.<schema>` strings. |

## Two-pass mental model

```
Pass 1 — DEPS (ensure_migrated):
  For every transitive %run / notebook.run target:
    if already in _migration_cache or already on cluster → SKIP
    else → migrate code only (Claude rewrites Databricks APIs)
           save .ipynb to <output-base>/<job>/deps/

Pass 2 — TASKS (per task in topo order):
  For each task notebook, for each code cell:
    1. Analyze (cell_plan: description, action, risks)
    2. Migrate (Claude with tool use rewrites)
    3. Execute on live cluster via WebSocket
    4. Verify:
        a. raised exception?
        b. error patterns in stdout? ("Error:", "Traceback", "FAILED")
        c. Spark logs show stage failure?
        d. Opus eval: does the output look correct?
    5. If any verify check failed → call_fix() with Claude + full tools.
       Up to 10 fix attempts per cell. fixup_cell can rewind to earlier indices.
  Save the fixed-up .ipynb to <output-base>/<job>/notebooks/...
  Emit JOB_REPORT.md
```

## Log patterns to watch for

When tailing `/tmp/migration.log`, key lines:

```
[12:34:56]   [<job>/<task>]    Cell 5/27: OK
[12:35:42]   [<job>/<task>]    Cell 12/27: OK (fixed attempt 2)
[12:36:18]   [<job>/<task>]    Cell 14/27: VERIFY FAIL (attempt 3/10): TABLE_OR_VIEW_NOT_FOUND
[12:39:01]   [<job>/<task>]    [child:helpers/io_utils.ipynb] Cell 3/8: OK
[12:42:15]   [<job>/<task>]    [fixup_cell] Rewinding to index 7 (reason: variable redefined upstream)
[12:48:30]   [<job>/<task>]    RESULT: PASS
```

`RESULT: PASS` → all cells executed cleanly. `RESULT: PARTIAL` → some cells failed all 10 attempts; review `JOB_REPORT.md`. `RESULT: FAIL` → catastrophic (cluster died, manifest broken).

## Output layout

After a successful run:
```
<output-base>/<job-name>/
  notebooks/Users/.../<notebook>.ipynb   ← the migrated, run-validated notebook
  deps/dep_<name>/<notebook>.ipynb       ← Pass-1 dep artifacts (informational)
  tasks/<numbered_key>/                  ← per-task reports
  reports/
  JOB_REPORT.md                          ← cell pass/fail/fix counts
```

The migrated `.ipynb`s are uploaded to your AIDP workspace at `<output-base>` AND saved to your local `oci-aidp-databricks-validator/reports/<job-name>/` for offline review.

## When it goes wrong

| Symptom | Skill / fix |
|---|---|
| `RESULT: PARTIAL` with N cells failing all 10 attempts | [`aidp-fixup-cell`](../aidp-fixup-cell/SKILL.md) for each. |
| Cluster died mid-run (WS disconnects) | Restart cluster. Re-invoke with `--skip-migrated` (default) — Pass-1 deps already done aren't repeated. |
| User wants to abort | `pkill -f job_migrate.py` (SIGTERM) — lets the current cell finish. |
| User wants to resume after manual fixes to a dep | [`aidp-resume-migration`](../aidp-resume-migration/SKILL.md). |
| Migrated table is in the redirect schema (`<sandbox>`) but user expected production location | Check [`references/gotchas.md`](../../references/gotchas.md) §"redirect schema". Re-run with `--no-redirect-schema` (USE WITH CARE — bypasses data-safety gate). |

## Safety notes the skill enforces

- **Write-redirect sandbox schema.** Every `.saveAsTable(...)` / `INSERT INTO` is silently rewritten to a sandbox `<schema>.<table>` location during migration. Source production data is never touched. The redirect schema is verified per-task (`databaseExists`) — if verification fails, the task fails fast.
- **No `--no-redirect-schema` without explicit user consent.** Bypassing the redirect drops the data-safety guarantee.
- **No `--skip-migrated=false` without explicit user consent.** Force-re-migration re-spends Claude tokens AND can overwrite manual fixes the user applied to a previously-migrated notebook.

## Cost / time guidance

- A typical 30-cell notebook takes ~5-15 minutes on a warm cluster, costs $1-3 in Claude tokens.
- A typical 5-task workflow with ~150 cells total: 30-90 min, $10-30 in Claude.
- Pass-1 deps are SHARED across jobs in the same run — second job is cheaper.

## After this

- Read the JOB_REPORT.md ([`/migration-status`](../../commands/migration-status.md) command auto-parses it).
- For any `PARTIAL` cells, route to [`aidp-fixup-cell`](../aidp-fixup-cell/SKILL.md).
- For streaming / batch convergence pipelines, follow up with [`aidp-acceptance-contract`](../aidp-acceptance-contract/SKILL.md).
