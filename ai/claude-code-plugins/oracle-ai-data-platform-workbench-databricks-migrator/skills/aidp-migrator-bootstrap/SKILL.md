---
name: aidp-migrator-bootstrap
description: One-shot environment readiness check for the Oracle AIDP Databricks migrator. Verifies Python deps, OCI auth, AIDP cluster reachability, and the customer's env-coords file. Use the first time the user invokes the migrator on a workstation, or when any other skill fails with an auth / connectivity error.
---

# `aidp-migrator-bootstrap` — environment readiness check

Confirms everything the migrator needs is in place before any of the other skills attempt real work. Idempotent — re-runnable whenever you suspect drift.

## When to use

- The user is running the migrator for the first time on this workstation.
- Any other skill fails with an OCI auth, network, or cluster-not-found error.
- The user moved to a new region / DataLake / workspace and you need to re-verify.

## Required env-coords

Before any check below works, the user MUST have an `env-coords.md` (or any plain notes file) listing **all of these**. Refuse to proceed and ask for them if any are missing — never guess.

| Coordinate | Example shape (do NOT use any of these literal values — these are placeholders) |
|---|---|
| AIDP REST base URL | `https://aidp.<region>.oci.oraclecloud.com/20240831` |
| DataLake OCID | `ocid1.aidataplatform.oc1.<region>.<id>` |
| Workspace UUID | `<8-4-4-4-12>` UUID format |
| Cluster ID | `<8-4-4-4-12>` UUID format |
| OCI profile name | `<your-profile>` (e.g. the section name in `~/.oci/config`) |
| Output workspace path | `Shared/aidp-migration-tool-output/` (or your team's path) |

Save these into a `env-coords.md` file at the project root, gitignored. Every other skill in this plugin threads these through verbatim. See [references/env-coords.template.md](../../references/env-coords.template.md) for a complete scaffold.

## Step-by-step

### 1. Python prereqs

The migrator engine ships bundled with this plugin under `${CLAUDE_PLUGIN_ROOT}/engine/`. Install its Python dependencies once:

```bash
python3 --version          # 3.10+
pip install -r ${CLAUDE_PLUGIN_ROOT}/engine/requirements.txt
```

Expected packages: `oci`, `requests`, `websocket-client`, `anthropic`, `cryptography`. If any is missing, install + retry.

### 2. `ANTHROPIC_API_KEY` env var

```bash
echo $ANTHROPIC_API_KEY | wc -c    # should be >50 chars
```

If empty, the migrator's Pass-2 cell-by-cell loop will crash. Ask the user to `export ANTHROPIC_API_KEY=sk-ant-...`.

### 3. OCI authentication

Two valid paths — confirm which the user is using:

**API key (recommended for unattended runs)**
```bash
oci iam region list --profile <profile-name>     # returns a region list on success
```

**Session token (interactive only, expires ~1 hr)**
```bash
oci session validate --profile <profile-name>
# if expired:
oci session authenticate --profile <profile-name> --region <region>
```

If either returns 401/403 or "Security Token expired", surface the exact command the user should run — do not pretend success.

### 4. AIDP cluster reachability + state

```bash
curl -s -H "..." \
  "<AIDP_BASE>/dataLakes/<DATALAKE_OCID>/workspaces/<WORKSPACE_UUID>/clusters/<CLUSTER_ID>" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('lifecycleState'))"
```

Expect `Active`. Acceptable transient states are `Starting`, `Updating`. If `Stopped` or `Failed`, instruct the user to start the cluster from AIDP console before any Pass-2 run.

> Curl with `auth=signer` (the migrator's helper) is the proper way; if the user wants an exact one-liner, suggest it in Python (fill the four placeholder values from your `env-coords.md`):
> ```python
> import oci, requests
>
> # Fill these from env-coords.md (see references/env-coords.template.md):
> profile = "<your-profile-name>"
> base    = "<AIDP_BASE>"          # e.g. https://aidp.<region>.oci.oraclecloud.com/20240831
> lake    = "<DATALAKE_OCID>"
> ws      = "<WORKSPACE_UUID>"
> cl      = "<CLUSTER_ID>"
>
> cfg = oci.config.from_file(profile_name=profile)
> signer = oci.signer.Signer(cfg["tenancy"], cfg["user"], cfg["fingerprint"], cfg["key_file"])
> r = requests.get(f"{base}/dataLakes/{lake}/workspaces/{ws}/clusters/{cl}", auth=signer)
> print(r.json().get("lifecycleState"))
> ```

### 5. Workspace write permission

Confirm the operator can write under the target output path:

```bash
python3 -c "
import oci, requests, urllib.parse, os
cfg = oci.config.from_file(profile_name='<profile>')
signer = oci.signer.Signer(cfg['tenancy'], cfg['user'], cfg['fingerprint'], cfg['key_file'])
base = '<AIDP_BASE>'
lake = '<DATALAKE_OCID>'
ws = '<WORKSPACE_UUID>'
path = '<output-workspace-path>'
r = requests.get(f'{base}/dataLakes/{lake}/workspaces/{ws}/objects',
                 params={'path': path}, auth=signer)
print(r.status_code, r.text[:200])
"
```

200 → good. 404 → ask the user to create the folder via AIDP console or via `POST .../objects` with `type=FOLDER`. 401/403 → the operator's OCI principal lacks `workspace.write` on this workspace.

### 6. Migrator pyc compile sanity

```bash
python3 -m py_compile scripts/build_dag.py scripts/check_data_availability.py \
                      scripts/job_migrate.py scripts/migrate_catalog.py
```

Should be silent. If a Python-version mismatch is reported, the user is on the wrong interpreter.

## Output template

After running, report back to the user as a single table:

```
| Check               | Status | Notes |
|---|---|---|
| Python deps         |  OK    | 3.13.x, all 5 deps present |
| ANTHROPIC_API_KEY   |  OK    | set, 100+ chars |
| OCI auth (api_key)  |  OK    | profile <name>, region <region> |
| Cluster state       |  OK    | Active |
| Workspace write     |  OK    | output base reachable |
| Migrator compile    |  OK    | all 4 entrypoints compile |
```

Any FAIL row → emit the exact command the user needs to run to fix it. Do not proceed to other skills if any check failed.

## Notes

- This skill never modifies anything. It is read-only.
- If the user is on a session token and it's about to expire (`oci session validate` shows <10 minutes remaining), warn them and suggest refresh BEFORE invoking a long-running skill like [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md).
- Re-run after any change in region, DataLake, workspace, or cluster.
