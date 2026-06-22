---
name: aidp-resume-migration
description: Resume an interrupted migration. The migrator caches already-migrated notebooks (in-memory + on-cluster) so subsequent runs skip them. Use when a prior aidp-migrate-job run was killed mid-flight (Ctrl-C, cluster restart, network drop) or when resuming after a manual fix to a specific dep notebook.
---

# `aidp-resume-migration` — pick up where you left off

The migrator is designed to be resumable. Each successful Pass-1 dep migration is cached in two places:
- **Module-level `_migration_cache` dict** in `job_migrate.py` (in-memory, per-run).
- **On-cluster `os.path.exists()` probe** against the output path (persistent across runs).

Pass-2 task notebooks are similarly skipped if their final `.ipynb` already exists at `<output-base>/<job>/notebooks/...`.

## When to use

- A prior [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) run was killed (Ctrl-C, cluster restart, network drop, timeout).
- The user manually edited a specific dep / task notebook and wants to resume without re-doing the rest.
- The user added a new task to the manifest and wants to migrate only the new task.

## Default behavior (resume is automatic)

```bash
python3 scripts/job_migrate.py \
  --manifest reports/<MyJob>_manifest.json \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --output-base <output-workspace-path> \
  --oci-profile <profile>
```

The default flag is `--skip-migrated` (ON). Every notebook already at the output path is silently skipped. So a plain re-invoke after a crash IS the resume — no special flag needed.

## Resuming from a specific task

If the failure was task-specific and you want to start there (skipping all earlier tasks):

```bash
python3 scripts/job_migrate.py \
  --manifest reports/<MyJob>_manifest.json \
  --start-task <substring_of_task_key>
  # + the rest of the standard args
```

`--start-task` is a substring match on `task_key`. Skip every task whose key sorts before the matched task.

## Resuming a single task (skip everything else)

```bash
python3 scripts/job_migrate.py \
  --manifest reports/<MyJob>_manifest.json \
  --only-tasks "<task_key_1>,<task_key_2>"
  # + the rest
```

Useful when you've manually fixed one dep and want to re-run only the tasks that consume it.

## Force re-migration of an already-migrated notebook

You typically DON'T want this — it burns Claude tokens. But if you must:

```bash
python3 scripts/job_migrate.py \
  --manifest reports/<MyJob>_manifest.json \
  --no-skip-migrated
  # + the rest
```

**Warning**: this overwrites any manual edits the user applied to the previously-migrated `.ipynb` at the output path. If the user has made manual fixes, REFUSE to use this flag without explicit "yes, overwrite my edits".

## When the cache lies

The cache can be wrong in these scenarios — clear it manually if you see them:

| Scenario | Fix |
|---|---|
| User edited the SOURCE Databricks notebook after the prior migration. The migrator still skips it (output already exists). | Delete the corresponding output `.ipynb` from AIDP workspace + re-run. |
| User modified the manifest to add a new dep, but a previously-migrated task still uses the OLD dep path. | Delete the output `.ipynb` for that task + re-run. |
| The output `.ipynb` was created but is empty / corrupt (cluster died mid-write). | Delete the bad file + re-run. |

There's no `--clear-cache` flag — the cache IS the filesystem state of `<output-base>`.

## Cluster state when resuming

Before invoking, verify the cluster is still Active (might have been auto-stopped for idle):

```bash
# (use the bootstrap skill's check)
```

If `Stopped`, start it via AIDP console. The first cell after resume will pay a connection-setup cost (~10-30s), then steady-state resumes.

## How to know what's already done

```bash
# List all migrated notebooks for this job
oci os object list ... (or use AIDP workspace listing)
ls reports/<job-name>/notebooks/   # local mirror after a successful run
```

Or just tail the log and look for `[SKIP] already migrated:` lines on resume.

## After this

- Same as [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) — read `JOB_REPORT.md`, route failed cells to [`aidp-fixup-cell`](../aidp-fixup-cell/SKILL.md), etc.
