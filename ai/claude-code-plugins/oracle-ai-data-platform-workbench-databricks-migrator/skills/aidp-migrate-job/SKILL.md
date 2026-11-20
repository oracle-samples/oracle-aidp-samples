---
name: aidp-migrate-job
description: Run the full Databricks→AIDP migration against a manifest. Pass-1 walks the %run dep tree and rewrites Databricks APIs in each dep notebook code-only. Pass-2 executes each task cell-by-cell on a live AIDP cluster, runs 4-way verify (exec error / stderr patterns / Spark logs / Opus eval), and re-attempts up to 5 times via Claude with tool use (default `MAX_FIX_ATTEMPTS`; tunable via `AIDP_MAX_FIX_ATTEMPTS` / `runtime.fix_max_attempts`). Use when the user is ready to actually port the workload (not just plan it). Long-running — typical job takes 10–60 minutes per task depending on cell count.
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

`job_migrate_from_workflow.py` is the canonical runner (it drives the core `job_migrate.py` library — do not invoke `job_migrate.py` directly):

```bash
export ANTHROPIC_API_KEY=sk-ant-...
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate_from_workflow.py \
  --config <customer>.yaml \
  --job-key <workflow-key>
```

Or against a pre-built manifest:
```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate_from_workflow.py \
  --config <customer>.yaml \
  --manifest reports/<MyJob>_manifest.json
```

The customer YAML (`--config`; falls back to `$AIDP_CONFIG_PATH`, `./aidp_migrator.yaml`, `~/aidp_migrator.yaml`) supplies the AIDP coordinates and `paths.migrate_output`; `--migrate-output <path>` is a per-run override (`--output-base` is a deprecated alias). **Never pass `--cluster`** — it is a deprecated, warned no-op; per-task clusters come from the workflow/manifest.

Tail the log in another terminal:
```bash
# the migrator streams to stdout; tee it yourself to follow from another shell:
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate_from_workflow.py ... 2>&1 | tee migration.log
```

## Useful flags

| Flag | When to use |
|---|---|
| `--start-task <substring>` | Resume from this task (skip everything before). Pairs with [`aidp-resume-migration`](../aidp-resume-migration/SKILL.md). |
| `--only-tasks <names>` | Run ONLY these specific tasks. Useful for re-running a single failed task. |
| `--skip-migrated` | Reuse notebooks already migrated in a prior run. **Default OFF** (fresh migration guarantees nested `%run` deps are converted); prefer `--start-task` to scope a resume. `--no-skip-migrated` states the default explicitly. |
| `--catalog-manifest <path>` | Apply deterministic source-catalog → `default` remap in string literals (the `--out-manifest` file from `migrate_catalog.py`). **Only on `job_migrate_from_workflow.py`** — the core `job_migrate.py` has no such flag. |

## Two-pass mental model

```
Pass 1 — DEPS (ensure_migrated):
  For every transitive %run / notebook.run target:
    if already in _migration_cache or already on cluster → SKIP
    else → migrate code only (Claude rewrites Databricks APIs)
           save .ipynb to <migrate-output>/<job>/deps/

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
       Up to 5 fix attempts per cell (MAX_FIX_ATTEMPTS; env AIDP_MAX_FIX_ATTEMPTS
       or runtime.fix_max_attempts in the YAML). fixup_cell can rewind to
       earlier indices.
  Save the fixed-up .ipynb to <migrate-output>/<job>/notebooks/...
  Emit JOB_REPORT.md
```

## Log patterns to watch for

Key lines in the migrator's output:

```
[12:34:56]   [<job>/<task>]    Cell 5/27: OK
[12:35:42]   [<job>/<task>]    Cell 12/27: OK (fixed attempt 2)
[12:36:18]   [<job>/<task>]    Cell 14/27: VERIFY FAIL (attempt 3/5): TABLE_OR_VIEW_NOT_FOUND
[12:39:01]   [<job>/<task>]    [child:helpers/io_utils.ipynb] Cell 3/8: OK
[12:42:15]   [<job>/<task>]    [fixup_cell] Rewinding to index 7 (reason: variable redefined upstream)
[12:48:30]   [<job>/<task>]    RESULT: PASS
```

`RESULT: PASS` → all cells executed cleanly. `RESULT: PARTIAL` → some cells failed all 5 attempts; review `JOB_REPORT.md`. `RESULT: FAIL` → catastrophic (cluster died, manifest broken).

## Output layout

After a successful run:
```
<migrate-output>/<job-name>/
  notebooks/Users/.../<notebook>.ipynb   ← the migrated, run-validated notebook
  deps/dep_<name>/<notebook>.ipynb       ← Pass-1 dep artifacts (informational)
  tasks/<numbered_key>/                  ← per-task reports
  reports/
  JOB_REPORT.md                          ← cell pass/fail/fix counts
```

The migrated `.ipynb`s are uploaded to your AIDP workspace at `<migrate-output>` AND saved to your local `./reports/<job-name>/` for offline review.

## When it goes wrong

| Symptom | Skill / fix |
|---|---|
| `RESULT: PARTIAL` with N cells failing all 5 attempts | [`aidp-fixup-cell`](../aidp-fixup-cell/SKILL.md) for each. |
| Cluster died mid-run (WS disconnects) | Restart cluster. Re-invoke with `--start-task <substring>` (or an explicit `--skip-migrated` for a confident resume — it is OFF by default). |
| User wants to abort | `kill <pid>` with the PID recorded at launch (SIGTERM) — lets the current cell finish. `pkill -f` would also hit other users' runs on a shared host. |
| User wants to resume after manual fixes to a dep | [`aidp-resume-migration`](../aidp-resume-migration/SKILL.md). |
| Migrated table is in the redirect schema (`<sandbox>`) but user expected production location | Expected: the write-redirect sandbox is mandatory during migration (there is NO bypass flag). Promote data to the production location as a deliberate post-migration step, outside the migrator. |

## Safety notes the skill enforces

- **Write-redirect sandbox schema.** Every `.saveAsTable(...)` / `INSERT INTO` is silently rewritten to a sandbox `<schema>.<table>` location during migration. Source production data is never touched. The redirect schema is verified per-task (`databaseExists`) — if verification fails, the task fails fast.
- **The redirect cannot be bypassed.** There is no `--no-redirect-schema` (or equivalent) flag — the data-safety gate is unconditional during migration.
- **Runtime sandbox gate on the cluster.** The bootstrap declares the redirect schema/bucket, the staging areas the migration rules themselves use (`/Volumes/default/default/dbfs/`, `/tmp/`) and the output directory as `AIDP_SANDBOX_CATALOG` / `AIDP_SANDBOX_SCHEMA` / `AIDP_SANDBOX_PREFIX` in the kernel, and freezes that declaration for the kernel. `aidp_compat` then refuses, before execution, any `dbutils.notebook.run` child cell that imports `subprocess` / network modules (also via `importlib`, `builtins`, `sys.modules` or an alias), reads `os.environ`, calls `shutil.rmtree` / `eval` / `exec`, imports or rebinds the compat shims / policy module, or opens, removes, renames or writes (`open`, `pathlib`, `os.remove`, `shutil.move`, `df.write.*`, `writeTo`, `spark.sql` DDL/DML, `dbutils.fs.*`) a target outside the sandbox (`PermissionError` naming notebook, cell index, rule id, remediation); targets that cannot be resolved statically are refused unless allowlisted; `..` segments never count as inside. `dbutils.fs.rm/mv/cp/put/mkdirs` + `safe_io` writers assert their targets at runtime. Refusals appear in the task test report under **Notebook Policy Log**. If a refusal is legitimate, the fix is a reviewed exception (`AIDP_NOTEBOOK_POLICY_ALLOW=<rule-id>`), an extra staging prefix (comma-separated `AIDP_SANDBOX_PREFIX`, set on the cluster before the bootstrap -- it replaces the whole list), or `AIDP_SANDBOX_ALLOW_NETWORK=1` -- never editing the gate. The gate is a static check of listed forms, not a Python sandbox; the runtime assertions are the enforcement layer for compat-helper writes (see `engine/aidp_compat/SUPPORTED_OPERATIONS.md` section 5 for what is not covered).
- **No plaintext secrets.** `dbutils.secrets` resolves from OCI Vault only. Do not set `AIDP_ALLOW_PLAINTEXT_SECRETS=1` on a cluster that runs customer notebooks; it is a local-demo switch. The bootstrap also restricts `dbutils.secrets` to the scope/key literals found while planning each task (`AIDP_SECRET_SCOPES` / `AIDP_SECRET_KEYS`; `none` before the first task). A `NBP-RUNTIME-SECRET` refusal in the Notebook Policy Log means a scope/key the plan did not see (for example a scope taken from a widget): the fix is an operator-provided `AIDP_SECRET_SCOPES` / `AIDP_SECRET_KEYS` on the cluster after review.
- **Re-migration overwrites manual edits — confirm first.** The default is a fresh migration (`--skip-migrated` is OFF), which re-spends Claude tokens AND can overwrite manual fixes the user applied to a previously-migrated notebook at the output path. If the user has manual edits, get an explicit "yes, overwrite" or scope the run with `--start-task` / `--only-tasks` / `--skip-migrated`.

## Cost / time guidance

- A typical 30-cell notebook takes ~5-15 minutes on a warm cluster, costs $1-3 in Claude tokens.
- A typical 5-task workflow with ~150 cells total: 30-90 min, $10-30 in Claude.
- Pass-1 deps are SHARED across jobs in the same run — second job is cheaper.

## After this

- Read the JOB_REPORT.md ([`/migration-status`](../../commands/migration-status.md) command auto-parses it).
- For any `PARTIAL` cells, route to [`aidp-fixup-cell`](../aidp-fixup-cell/SKILL.md).
- For streaming / batch convergence pipelines, follow up with [`aidp-acceptance-contract`](../aidp-acceptance-contract/SKILL.md).
