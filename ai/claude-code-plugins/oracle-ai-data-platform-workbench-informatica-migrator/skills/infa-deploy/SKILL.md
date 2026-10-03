---
name: infa-deploy
description: Upload generated notebooks and create AIDP jobs via `infa2aidp cli deploy` (OCI-signed REST calls — upload files, create job objects). Use once notebooks from infa-migrate-mapping have been reviewed and are ready to land in an AIDP workspace. The upload and job-creation calls are verified against a live AIDP instance; the command still does NOT execute the notebook or verify it produces correct data — that gap is called out below. Requires a USER cluster, not the workspace default.
---

# `infa-deploy` — upload notebooks, create AIDP jobs

Runs `infa2aidp cli deploy`. Uploads the `.ipynb` notebooks
[`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md) generated into an
AIDP workspace and creates one AIDP job per generated `workflows/<name>.json`.

## When to use

- Notebooks have passed [`infa-review`](../infa-review/SKILL.md) and the
  user is ready to land them in AIDP.
- The user says "push these to AIDP", "deploy the notebooks", "create the
  jobs".

## Important — what this command does not do

`infa-deploy` is a control-plane operation: it uploads files and creates
job definitions via REST. **It does not execute anything on a live
cluster, and it does not verify the uploaded notebook actually runs.**
There is no cell-by-cell execute/verify/fix loop in this tool. After
`infa-deploy` succeeds, the notebook and job exist in AIDP; whether the
notebook *runs correctly* is unknown until someone runs the job and
[`infa-reconcile`](../infa-reconcile/SKILL.md) checks the result.

### What has been verified live, and what has not

Run against a live AIDP instance on 2026-09-25 (Spark 3.5.0):

| Step | Verified |
| --- | --- |
| OCI-signed auth, workspace listing | yes |
| `actions/mkdir`, three-step `uploadFileMeta` PAR upload | yes — 12 notebooks, 0 failures |
| Notebook readable back from the workspace | yes |
| `POST .../jobs` job creation | yes — job key returned |
| Re-deploy updating the job in place | yes — `PUT .../jobs/{key}` |
| Job list paging when looking a job up by name | yes — 25 per page, `limit` <= 100, `opc-next-page` |
| Job **run** reaching success | yes — a single notebook 2026-09-25, and all 12 corpus notebooks as one job (11 end to end) |
| Notebook reading real tables, writing a Delta target | yes |
| Output matching independently computed ground truth | yes — one mapping, hand-computed before the run |
| `schedule` `{quartzCronExpression, timezoneId, pauseStatus}` | yes — created `PAUSED`; a manual run of a paused job works |
| Task `parameters` `[{name, value}]`, read in the notebook | yes — via `oidlUtils.parameters.getParameter(name, default)` |
| `dependsOn` task ordering | yes — second task started 1 s after the first finished |
| `infa_compat` as a cluster library, and via `sys.path` | yes — both routes, inside job runs |

So the request shapes are no longer a guess, and generated notebooks have
run. Every corpus notebook was executed as an AIDP job against scratch
Delta tables built from each mapping's own source definitions, 11 of 12 end
to end; and one mapping's output was reconciled against numbers computed by
hand before the run (see the CHANGELOG's "Live
corpus run").

What is still unproven is real data. No generated notebook has read a
customer source, written a production table, or been reconciled against
Informatica executing the same mapping -- the one reconciliation was
against hand-computed numbers.

Two prerequisites those runs surfaced: the cluster needs `infa_compat`,
either installed as a `WORKSPACE_FILE` library or imported from the
`/Workspace` mount via `sys.path`; and a target that does not yet exist is
now created empty from the batch schema with a REVIEW marker, because its
column types then come from the DataFrame rather than the export.

**The first live run found a defect the tests could not.** The deployer
accepted the workspace's DEFAULT master-catalog cluster, created the job,
and AIDP failed the run:

```
WORKFLOW_EXECUTION_0071 - Default Cluster "Default Master Catalog Compute"
used for non-system task s_m_EmployeeSummary. Change the cluster before
workflow execution.
```

The request shape was valid, so `tests/test_deployer.py`'s fake client
asserted it and passed. Only the service knows a cluster's `type`. The
deploy now refuses a `DEFAULT` cluster **before uploading anything** —
a job that can never run is a failed deploy, not a partial success.

Still run `--dry-run` first, and still treat a live API-shape error as a
defect report rather than a one-off.

## Prerequisites the user must have

1. **OCI credentials** in `~/.oci/config` (API key or a session-token
   profile). Never paste a key or token into chat or onto a command line.
2. The **DataLake OCID** (`AIDP_INSTANCE_ID`), **region** (`AIDP_REGION`),
   **workspace key** (`AIDP_WORKSPACE_KEY`) and the **cluster key** the jobs
   should run on (`AIDP_CLUSTER_KEY`). The cluster must be a **USER**
   cluster: AIDP will not run a non-system job task on the workspace's
   default master-catalog compute, and the deploy refuses that key rather
   than creating a job that fails every run.
3. **`infa_compat` installed on that cluster.** Every generated notebook
   imports it and asserts its version in its second cell. Build the wheel
   with `pip wheel engine/ -w dist/` and install it as a cluster library
   (AIDP console → cluster → Libraries, or the `aidp-cluster-ops` skill).
   Without it every notebook fails before its first read. On a shared
   cluster you can instead upload the wheel under the deployment folder
   and `sys.path.insert(0, "/Workspace/<folder>/<wheel>")` in a first
   cell -- the workspace is mounted on the driver (see README, "Installing
   infa_compat on the cluster").

Scheduled workflows are created with `pauseStatus: PAUSED` -- the
schedule's timezone is an assumption (PowerCenter records none), so confirm
it and unpause the job in AIDP after review. Per-run parameter overrides are
AIDP job or task parameters named `migration.<name>` (or `<NAME>`); the
notebook reads them with `oidlUtils.parameters.getParameter`, not from
`spark.conf`.

Two things a real deployment taught us (2026-09-24): re-running `deploy`
updates an existing job only under `--overwrite` (otherwise it is reported
as skipped, never duplicated), and from Git Bash on Windows you must pass
`MSYS_NO_PATHCONV=1` so `/Workspace/...` is not rewritten into a Windows
path -- the tool refuses any workspace path outside `/Workspace`.

## Try dry-run first

```bash
PYTHONPATH=engine python3 -m infa2aidp.cli deploy \
  -i <migrate-output-dir> --dry-run
```

`--dry-run` needs no credentials and makes no network call. It lists every
`.ipynb` it would upload (with the remote path) and every job it would
create. If it reports `Uploaded: 0`, the input directory is not a
`migrate` output directory.

## Canonical invocation

```bash
export AIDP_REGION=us-ashburn-1
export AIDP_INSTANCE_ID=ocid1.aidataplatform.oc1.iad....
export AIDP_WORKSPACE_KEY=<workspace-key>
export AIDP_CLUSTER_KEY=<cluster-key>
export OCI_PROFILE=DEFAULT
PYTHONPATH=engine python3 -m infa2aidp.cli deploy \
  -i <migrate-output-dir> \
  --workspace-path /Workspace/Migrated
```

## Flags

| Flag | Default (or env fallback) | Notes |
|---|---|---|
| `-i, --input` | required | The `migrate` output directory — `.ipynb` notebooks under `<folder>/`, and a `workflows/` subdirectory if present |
| `-o, --output` | `<input>/reports` | Folder for `deploy_report.md` |
| `--region` | `AIDP_REGION` | OCI region of the DataLake; required unless `--dry-run` |
| `--instance-id` | `AIDP_INSTANCE_ID` | DataLake OCID; required unless `--dry-run` |
| `--workspace-key` | `AIDP_WORKSPACE_KEY` | Required unless `--dry-run` |
| `--profile` | `OCI_PROFILE` (default `DEFAULT`) | `~/.oci/config` profile used for request signing |
| `--cluster-key` | `AIDP_CLUSTER_KEY` | Cluster every created job task runs on; without it notebooks upload but job creation is reported as failed |
| `--workspace-path` | `AIDP_WORKSPACE_PATH` (default `/Workspace/Migrated`) | Notebooks land under `<path>/<folder>/` |
| `--overwrite` | off | Replace notebooks already at the target path and update existing jobs in place (without it both are reported as skipped) |
| `--dry-run` | off | No upload, no credentials required |

## Output

Console: `Uploaded: N  Updated: U  Failed: M  Skipped: K` (plus `Would
deploy: D` and `DRY RUN -- nothing was deployed` on a dry run). Writes
`deploy_report.md` under `<output>/` (default `<input>/reports/`) with
the remote path of every notebook, the job key of every created job, and a
pointer to each workflow's `.review.md` when the source workflow did not
translate completely. Exit code 1 if any upload or job creation failed.

## When it goes wrong

| Symptom | Fix |
|---|---|
| `AIDP deploy needs --region / AIDP_REGION, ...` (exit 1) | Not in `--dry-run` mode and a required setting is missing — set it or add `--dry-run`. |
| `Uploaded: 0` in dry run | `-i` is not a `migrate` output directory (no `.ipynb` files under it). |
| 401 / `NotAuthenticated` | The OCI profile has no valid credential; for a session-token profile run `oci session authenticate --profile <profile> --region <region>`. |
| Jobs `failed -- A cluster key is required` | Pass `--cluster-key` / set `AIDP_CLUSTER_KEY`. |
| `ValueError: ... is the workspace's DEFAULT master-catalog compute` | Refused on purpose, before any upload. Pass a USER cluster. `Default Master Catalog Compute` cannot run job tasks. |
| Run fails `WORKFLOW_EXECUTION_0071` | The job was created against a DEFAULT cluster by an older build. Re-deploy with a USER cluster key. |
| Jobs `failed -- JOB_VALIDATE_0031 ... already exists` | An older build. Current builds update the job with `--overwrite`, or report it `skipped` without. |
| Workflow status `skipped` | A job of that name exists and `--overwrite` was not passed. The existing definition may be stale. |
| Notebook run fails immediately, no trace in the run API | Almost always `infa_compat` missing from the cluster. The run API exposes no `errorTrace`; open the notebook in the UI to see the cell error. |
| Uploads succeed but the job never runs | Expected — `infa-deploy` creates the job, it does not trigger a run. Trigger it in AIDP, then reconcile. |
| Notebook fails in its second cell with `infa_compat` | The wheel is not installed on the cluster, or a different version is — see Prerequisites. |
| Some notebooks skipped | Check `--overwrite` — without it, a notebook already at the target path is skipped rather than replaced. |
| `workspace_path must be /Workspace or below it` | Git Bash rewrote the path: run with `MSYS_NO_PATHCONV=1` or pass `//Workspace/...`. |
| Scheduled job never fires | Expected until reviewed: schedules are created `PAUSED`. Confirm the timezone, then unpause the job in AIDP. |

## After this

The deployed notebook still needs to actually run (outside this tool's
scope) before [`infa-reconcile`](../infa-reconcile/SKILL.md) has anything
meaningful to compare against.
