# Laptop-CLI REST dispatch — operator setup

> **Scope**: setup notes for `aidp-fusion-autopilot run --mode seed` (no `--inline`) — the laptop-terminal path that dispatches the orchestrator notebook to a live AIDP cluster over signed REST.
>
> For in-notebook execution (the `--inline` path), no OCI setup is needed — the AIDP runtime injects `spark`, `aidputils`, and credentials as globals.

## What runs where

```
laptop terminal                          AIDP cluster
┌─────────────────────────────┐          ┌──────────────────────────┐
│  aidp-fusion-autopilot run     │          │  Spark + orchestrator    │
│    --mode seed --env dev    │   REST   │  (bronze → silver → gold)│
│                             │   ───→   │                          │
│  ├─ load bundle.yaml        │          │  emits AIDP_LIVE_TEST_   │
│  ├─ preflight (5 checks)    │          │  RESULT_BEGIN/END marker │
│  ├─ build wheel (cached)    │          │  with full RunSummary    │
│  ├─ inline wheel in 4-cell  │   ←───   │  JSON payload            │
│  │  notebook                │  marker  │                          │
│  ├─ POST /jobs + /jobRuns   │          │                          │
│  ├─ poll status             │          │                          │
│  └─ render RunSummary       │          │                          │
└─────────────────────────────┘          └──────────────────────────┘
```

## One-time setup

### 1. OCI CLI

Two signer flavors work — pick one:

#### (a) Session-token profile (recommended for laptops)

```bash
oci session authenticate --profile-name AIDP_SESSION --region us-ashburn-1
```

This opens a browser SSO flow. The CLI drops the token + key under `~/.oci/sessions/AIDP_SESSION/`. Token lifetime is typically 1 hour; refresh with:

```bash
oci session refresh --profile AIDP_SESSION
```

`aidp-fusion-autopilot`'s preflight runs `oci session validate --profile <name>` and surfaces a copy-paste `oci session refresh` hint if the token is expired.

#### (b) API-key profile (for unattended / CI usage)

Generate an API signing keypair, upload the public key under your OCI user, and write `~/.oci/config`:

```ini
[AIDP_PROFILE]
user=ocid1.user.oc1..xxx
fingerprint=aa:bb:cc:dd:...
tenancy=ocid1.tenancy.oc1..yyy
region=us-ashburn-1
key_file=~/.oci/oci_api_key.pem
```

API-key signatures don't expire on a clock; the AIDP plane verifies them end-to-end on the first preflight HTTP call.

> **Not supported in P1.5ε**: `auth.mode: vault` (cloud-side resource-principal / instance-principal signers). Preflight rejects it with a hint pointing at follow-up ticket `P1.5ε-fix6`.

### 2. AIDP IAM grants

The OCI user behind your profile must have:

- `read aiDataPlatformWorkspaces`
- `manage aiDataPlatformClusters` (for auto-start)
- `manage aiDataPlatformJobs`
- `use aiDataPlatformCredentials` (so the cluster notebook can load `FUSION_BICC_PASSWORD`)

If preflight fails check 4 (AIDP control plane reachable) with a 401, the OCI user lacks the workspace-level grant.

### 3. `aidp.config.yaml` dispatch coords

Fill in the five fields per environment:

```yaml
environments:
  dev:
    workspaceKey: <workspace-uuid>
    region: us-ashburn-1
    ociProfile: AIDP_SESSION

    # Dispatch coords (P1.5ε)
    aiDataPlatformId: ocid1.datalake.oc1.iad.<tenancy-specific>
    clusterKey: <cluster-uuid>
    clusterName: fusion_autopilot_dev
    biccSecretName: fusion_bicc_password   # optional; defaults to "fusion_bicc_password"
    biccSecretKey: password                # optional; defaults to "password"
```

#### Finding `aiDataPlatformId`

Open the AIDP console → Workspaces; the URL contains the data-platform OCID.

#### Finding `clusterKey`

```bash
oci raw-request --http-method GET \
  --target-uri "https://datalake.<region>.oci.oraclecloud.com/20260430/aiDataPlatforms/<aidp-id>/workspaces/<workspace-key>/clusters"
```

Each entry in `data.items[]` has a `key` (UUID) and `displayName`. Match by name; copy the `key`.

### 4. AIDP credential-store entry

The notebook's creds-cell calls `aidputils.secrets.get(name=<biccSecretName>, key=<biccSecretKey>)` to load the Fusion BICC password. Create the entry via the AIDP UI before the first run:

- **Name**: `fusion_bicc_password` (or whatever you set in `biccSecretName`)
- **Key**: `password` (or `biccSecretKey`)
- **Value**: your Fusion BICC user's password

> **Verified by preflight check 5 (P1.5ε-fix1, 2026-06-03)**: the dispatcher verifies this entry exists in the AIDP credential store before any wheel build, notebook upload, job submission, **or cluster start**. The check sits AHEAD of the cluster-state check so a missing credential fast-fails in ~300ms without paying ~5min cluster cold-start. A missing entry surfaces with the offending secret name + the configured `biccSecretKey` + a remediation hint pointing at the AIDP UI and `environments.<env>.biccSecretName` in `aidp.config.yaml`. See `tests/live/TC29_rest_dispatch.md` §"Probe 6" for the live evidence (6a happy path, 6b missing-credential fast-fail, 6c custom-key remediation).
>
> The credential store is **per-AIDP / per-data-lake**, NOT per-workspace — all workspaces under the same `aiDataPlatformId` share one store. Operators with multiple workspaces against the same AIDP only need to register the entry once.

## First run

```bash
$ aidp-fusion-autopilot run --mode seed --env dev
[preflight] PASS bundle.yaml: loaded bundle.yaml
[preflight] PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
[preflight] PASS OCI profile: session-token profile 'AIDP_SESSION' valid
[preflight] PASS AIDP control plane: reachable; 2 cluster(s) visible
[preflight] PASS BICC credential: credential 'fusion_bicc_password' present in AIDP store
[preflight] PASS cluster state: cluster '...' ACTIVE
[dispatch] wheel cache hit
[dispatch] notebook uploaded to /Workspace/Shared/aidp-fusion-autopilot-<project>/run.ipynb
[dispatch] jobKey=...
[dispatch] jobRunKey=...
[dispatch] status=PENDING
[dispatch] status=RUNNING
[dispatch] status=SUCCESS
[dispatch] orchestrator run_id=...
<RunSummary table>
```

Exit codes:

- `0` — every step succeeded.
- `1` — dispatch round-trip completed but one or more orchestrator steps failed (run summary rendered).
- `2` — dispatch-layer error: bad config, preflight failure, network, missing marker, etc. The red one-liner starts with a `DISPATCH_*` code.

## Smoke probes

Test the wiring without burning a full cluster cold-start + extract:

```bash
# Preflight + plan resolution; no wheel build, no upload, no dispatch.
aidp-fusion-autopilot run --mode seed --env dev --dry-run

# Validate the bundle without touching AIDP at all.
aidp-fusion-autopilot validate
```

Sample `--dry-run` output (P1.5ε-fix9 lands the laptop-side plan rendering — pre-fix9 the REST path returned `Empty plan` even for non-empty bundles):

```text
 PASS bundle.yaml: loaded ./bundle.yaml
 PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
 PASS OCI profile: API-key profile 'DEFAULT' loaded
 PASS AIDP control plane: reachable; 8 cluster(s) visible
 PASS BICC credential: credential '<your-secret-name>' present in AIDP store
 PASS cluster state: cluster '<your-cluster-uuid>' ACTIVE
dry-run requested — skipping wheel build + upload + dispatch
Dry-run plan for project <your-project> (mode=seed):
        Would dispatch
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┓
┃ dataset_id         ┃ layer  ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━┩
│ erp_suppliers      │ bronze │
│ ap_invoices        │ bronze │
│ dim_supplier       │ silver │
│ supplier_spend     │ gold   │
└────────────────────┴────────┘
```

Pass `--layers gold` to surface the `Extra-plan prerequisites` table — bronze + silver upstreams of in-plan gold marts that must exist on disk before the dispatch runs. Plan-resolution is mode-neutral (the same plan renders for `--mode seed` and `--mode incremental` — only per-step dispatch behavior differs).

## Diagnosing failures

The dispatcher saves the executed notebook for offline diagnosis on every run (success or failure). Look under `~/.aidp/dispatch/<run_id>/` if the dispatcher writes there (P3.x polish — not yet auto-emitted in this PR).

For now: the AIDP console's Job Runs tab carries the same executed-notebook view. Match by `jobRunKey` (printed during dispatch).

## Cross-references

- Plan: `docs/features/p1.5e-cli-rest-dispatch/plan.md`
- Post-ship UX bundle (`--poll-timeout`, diagnose-on-timeout, credential preflight): `docs/features/p1.5e-postship-dispatch-ux/plan.md`
- Skill: `.claude/skills/fusion-tc26-run/` — the empirical-probe harness this CLI productizes
- REST contract: `skills/aidp-rest/SKILL.md` — the gotchas baked into `AidpRestClient`
- Auth mode (`vault` deferred): tracked as `P1.5ε-fix6`
- BICC credential preflight: **shipped** as `P1.5ε-fix1` (2026-06-03)
- REST-dispatch `--resume`: **shipped** as `P1.5ε-fix5` (2026-06-03) — live evidence in `tests/live/TC29b_resume_via_rest.md`
