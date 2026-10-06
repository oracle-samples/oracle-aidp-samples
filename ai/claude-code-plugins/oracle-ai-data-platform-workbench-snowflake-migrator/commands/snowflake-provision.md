---
description: Provision the AIDP migration environment - workspace (name auto-translated), migration-assets cluster, cluster libraries, the backup-snowflake-migration/ folder with scripts and plan, and the migration jobs (discover, structure, reconcile, and one copy job per schema of the approved plan, passing schema as a task parameter). Dry-run by default; --execute only after showing the plan.
---

# `/snowflake-provision`

Thin wrapper over
[`snowflake-provision-environment`](../skills/snowflake-provision-environment/SKILL.md).

1. Ask for the aiDataPlatform OCID and the workspace name **in this turn**;
   the external/target catalog names if already decided.
2. Dry-run `snowmig.py provision` and show `PROVISION.md` — including any
   name translation and the API-contract note.
3. On the user's go-ahead, re-run with `--execute`.
4. Report each step's `verified` from `provision_result.json` — pending is
   pending, never rounded up.
5. Hand-off: the CLI and `PROVISION.md` print the workspace and cluster
   keys (recorded as `workspace.key` and `cluster.key` in
   `provision_result.json`; the display names are not keys). Pass them as
   `--workspace` / `--cluster-id` on every later command, or have the user
   put them in the config's `aidp:` block — one or the other — before
   `/snowflake-catalog`.
6. The plan push (S9/S10) re-runs this stage against the same workspace:
   `provision --execute --reuse-existing --workspace-name <the S1 name>
   --plan-label FULL|REDUCED`. Report any copy job listed as `stale` (exit 1)
   and offer `--delete-stale-copy-jobs` to remove it (it removes only a job
   this migration's records show it created; others stay listed as stale).
