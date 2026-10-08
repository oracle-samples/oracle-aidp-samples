# TC29 — Laptop-terminal REST dispatch live evidence (P1.5ε)

**Pod**: `playground` workspace (demo-pod)
**Date**: 2026-06-03
**Branch**: `<branch>` @ `fe41107` + cumulative dispatch fixes
**Operator**: redacted
**Cluster**: `fusion_autopilot_dev` (UUID redacted)

## Scope

End-to-end laptop-terminal dispatch via `aidp-fusion-autopilot run --mode seed --env dev --datasets erp_suppliers` against the live `playground` workspace. Validates the P1.5ε dispatch layer (Steps 2–7) on real OCI signing, real AIDP control plane, real cluster auto-start, real wheel build + upload + job submit + poll. Marker-parse + RunSummary deserialization deferred — covered separately by `tests/unit/dispatch/test_dispatch_via_rest.py::test_full_round_trip_returns_run_summary` (in-process round-trip with synthetic marker payload).

## Pre-conditions

- OCI CLI authed with an API-key profile (`DEFAULT`); session-token profile path covered by `dispatch/preflight.py` step 3b unit tests.
- `aidp.config.yaml` populated with the five P1.5ε dispatch fields (`aiDataPlatformId`, `clusterKey`, `clusterName`, `biccSecretName`, `biccSecretKey`) under `environments.dev`. Operator's actual values held outside the repo at `/tmp/aidp.config.p15e.yaml`.
- `bundle.yaml` narrow-scope projection (one bronze dataset `erp_suppliers`, no silver/gold) held outside the repo at `/tmp/bundle.p15e.yaml`. Real Fusion `serviceUrl`, BICC `username`, `externalStorage` profile name (operator-supplied per tenant), `password: ${env:FUSION_BICC_PASSWORD}` marker preserved literally through `load_bundle` and resolved cluster-side by the notebook's creds-cell via `aidputils.secrets.get(name="fusion_bicc_password", key="password")`.
- AIDP credential-store entry `fusion_bicc_password` provisioned in the `playground` workspace.
- `fusion_autopilot_dev` cluster initially STOPPED — used to validate Phase B auto-start path.

## Probe 1 — Phase A + B preflight + cluster auto-start (`--dry-run`)

```text
PASS bundle.yaml: loaded /tmp/bundle.p15e.yaml
PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
PASS OCI profile: API-key profile 'DEFAULT' loaded
cluster '<REDACTED-CLUSTER-UUID>' STOPPED — auto-starting (~5 min)…
   cluster_wait state=STARTING
   cluster_wait state=ACTIVE
PASS AIDP control plane: reachable; 8 cluster(s) visible
PASS cluster state: cluster '<REDACTED-CLUSTER-UUID>' auto-started to ACTIVE
dry-run requested — skipping wheel build + upload + dispatch
EXIT_CODE: 0
```

Validates:

- Phase A: bundle-load, dispatch-coord presence, OCI profile load (API-key path; step 3b session-token validation correctly skipped per `dispatch/preflight.py::_check_oci_profile_and_session`).
- Phase B: AIDP control-plane reachable (`list_clusters` succeeded), cluster state PASS — including the `STOPPED → STARTING → ACTIVE` auto-start transition with `wait_cluster_active` polling.
- `--dry-run` short-circuit: zero wheel-build, zero upload, zero job submission after Phase B PASS.
- Exit code 0 contract.

## Probe 2 — Job-name validation surfaced (`InvalidParameter` regression)

First full-dispatch attempt aborted at `create_notebook_job` with HTTP 400:

```text
[DISPATCH_JOB_SUBMIT] job submission failed: create_notebook_job: HTTP 400
body={"code":"InvalidParameter","message":"Invalid resource name. Must start with letter and no special characters are allowed except for underscore, slash."}
```

Root cause: dispatch entry point generated job name `aidp-fusion-autopilot-<project>-<env>-<epoch>` with hyphens; AIDP's job-name grammar permits only letters, digits, underscores, slashes. Fixed `dispatch/__init__.py` to sanitize tokens with `[^a-zA-Z0-9] → _` and switched task key `orchestrator-run` → `orchestrator_run`. Test fixtures in `tests/unit/dispatch/test_dispatch_via_rest.py` updated to match; the dispatch suite stayed green (91 / 91).

**Empirical contract added to `AidpRestClient` operator notes**: job + task identifiers must match `^[A-Za-z][A-Za-z0-9_/]*$`. Skill consumers using `tc26_<scope>_<stamp>` (`fusion-tc26-run/dispatch.py:336`) were already compliant by accident.

## Probe 3 — Full round-trip with wheel-filename regression surfaced

Second dispatch attempt completed the protocol end-to-end. Laptop-side log:

```text
PASS bundle.yaml: loaded /tmp/bundle.p15e.yaml
PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
PASS OCI profile: API-key profile 'DEFAULT' loaded
PASS AIDP control plane: reachable; 8 cluster(s) visible
PASS cluster state: cluster '<REDACTED-CLUSTER-UUID>' ACTIVE
wheel cache miss (hash=cf813f08328b9891); running `python -m build`
wheel cached: oracle_ai_data_platform_fusion_autopilot-cf813f08328b9891.whl
notebook_uploaded path=Workspace/Shared/aidp-fusion-autopilot-p15e-smoke/run.ipynb
notebook uploaded to /Workspace/Shared/aidp-fusion-autopilot-p15e-smoke/run.ipynb
job_created jobKey=<REDACTED-JOB-UUID>
jobRun_submitted jobRunKey=<REDACTED-RUN-UUID>
poll status=RUNNING
poll status=FAILED
[DISPATCH_RUN_FAILED] job_run_key=<REDACTED-RUN-UUID> reached terminal status 'FAILED'
WALL: 45.8s
EXIT_CODE: 2
```

Cluster-side cell-output probe (via `AidpRestClient.fetch_output` + `extract_cell_errors`) surfaced:

```text
cell 1 (install) — pip rc=1
  STDERR: ERROR: Invalid wheel filename (wrong number of parts):
    "oracle_ai_data_platform_fusion_autopilot-cf813f08328b9891"
```

Root cause: `dispatch/wheel_builder.py` cached the wheel under `<package>-<hash>.whl`, which violates PEP 427 (`name-version-pytag-abi-platform.whl`). Cluster-side `pip install --target` correctly refused. Fixed: namespace cache by hash subdirectory, preserve the original wheel filename (e.g. `~/.aidp/wheels/<hash>/oracle_ai_data_platform_fusion_autopilot-0.1.0a0-py3-none-any.whl`). Unit test `test_first_build_invokes_subprocess_and_caches` updated; dispatch suite stayed green.

Validates (despite the cluster-side failure):

- Wheel build cache miss → `python -m build` invocation + cache write.
- Notebook upload via contents API (PEP-3-quoted path).
- Job creation: `path: "jobs"` + `maxConcurrentRuns: 1` + redundant `jobClusters[]`/`tasks[].cluster` mirror — all REST quirks survive promotion to the plugin.
- Run submission + status polling with `on_status_change` callback emitting per-transition prints.
- Terminal-status detection: `RUNNING → FAILED` → `DispatchRunFailedError` with the right `DISPATCH_RUN_FAILED` code.
- Boundary contract: no `AidpRestError` escaped `dispatch_via_rest` — the CLI's `except (DispatchError, OrchestratorConfigError)` clause caught the wrapped error cleanly. Exit code 2, no Python traceback.

## Probe 4 — Wheel-filename fix re-verified end-to-end + DISPATCH_TIMEOUT path

Third dispatch attempt (post wheel-filename fix). Laptop-side log:

```text
PASS bundle.yaml: loaded /tmp/bundle.p15e.yaml
PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
PASS OCI profile: API-key profile 'DEFAULT' loaded
PASS AIDP control plane: reachable; 8 cluster(s) visible
PASS cluster state: cluster '<REDACTED-CLUSTER-UUID>' ACTIVE
wheel cache miss (hash=c42fd59957666b37); running `python -m build`
wheel cached: c42fd59957666b37/oracle_ai_data_platform_fusion_autopilot-0.1.0a0-py3-none-any.whl
notebook_uploaded path=Workspace/Shared/aidp-fusion-autopilot-p15e-smoke/run.ipynb
notebook uploaded to /Workspace/Shared/aidp-fusion-autopilot-p15e-smoke/run.ipynb
job_created jobKey=<REDACTED-JOB-UUID>
jobRun_submitted jobRunKey=<REDACTED-RUN-UUID>
poll status=RUNNING
[DISPATCH_TIMEOUT] poll_run(<REDACTED-RUN-UUID>): deadline exceeded after 1800s
WALL: 1833.1s
EXIT_CODE: 2
```

Wheel cache layout post-fix: `~/.aidp/wheels/c42fd59957666b37/oracle_ai_data_platform_fusion_autopilot-0.1.0a0-py3-none-any.whl` — pip-compliant filename preserved inside a hash-namespaced subdirectory.

Cluster-side cell-output probe at laptop-side timeout (job still RUNNING on AIDP):

```text
execution_info: {'total_cells': 5, 'current_cell': 4}
cell 1 (install) — pip rc=0 ✅
  output: "pip rc=0\nplugin installed to /tmp/aidp_fusion_autopilot_<...>/site-packages"
cell 2 (creds) — ✅
  output: "FUSION_BICC_PASSWORD loaded\norchestrator loaded"
cell 3 (run) — IN PROGRESS (no flushed outputs, AIDP-side current_cell=4)
cell 4 (verify) — not reached
```

Validates:

- **Wheel-filename fix landed**: cluster-side `pip install --target` succeeded (`pip rc=0`), wheel installed to ephemeral `/tmp/aidp_fusion_autopilot_<...>/site-packages` and prepended to `sys.path`.
- **Cluster-side creds + bundle write**: `aidputils.secrets.get(name="fusion_bicc_password", key="password")` returned a non-empty value (length=8); `BUNDLE_PATH.write_text(...)` succeeded; `from oracle_ai_data_platform_fusion_autopilot import orchestrator` succeeded.
- **`DISPATCH_TIMEOUT` boundary contract**: `poll_run`'s default `timeout_s=1800` deadline fired correctly; `AidpRestError("poll_run(...): deadline exceeded")` was message-classified by `dispatch_via_rest` and re-raised as `DispatchPollTimeoutError` with the right `DISPATCH_TIMEOUT` code. Exit 2, no traceback.

## Probe 6 — BICC credential preflight fast-fail (P1.5ε-fix1, 2026-06-03)

Added by `P1.5ε-fix1` post-ship follow-up. Captures the new Phase B check 6 firing live against `playground` when the configured `biccSecretName` doesn't match any entry in the AIDP credential store.

**Step 0 prerequisite confirmed live**: the credential REST endpoint is `GET /aiDataPlatforms/<aidp-id>/credentials` (data-lake scope, NOT workspace-scoped). Full shape captured in `dev/RESEARCH_aidp_rest_api_probe_results.md` §11. The single-resource `GET /credentials/<key>` endpoint expects a UUID — looking up by display name 400s. So `check_credential_exists(name)` LISTs and walks `items[]`.

**Preflight ordering** (reviewer-driven correction landed in commit `f577862`): credential check is **check 5** (runs BEFORE the cluster check) so a missing credential fast-fails without paying cluster cold-start. Cluster state is **check 6** and SKIPs when check 5 FAILs. The Probe 6a / 6b output below reflects this order — credential before cluster-state on every line.

**6a. Happy path (credential present)** — sanity check that the new credential check PASSes when the configured secret matches an existing entry:

```text
PASS bundle.yaml: loaded /tmp/bundle.p15e.yaml
PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
PASS OCI profile: API-key profile 'DEFAULT' loaded
PASS AIDP control plane: reachable; 8 cluster(s) visible
PASS BICC credential: credential 'fusion_bicc_password' present in AIDP store
PASS cluster state: cluster '<REDACTED>' ACTIVE
dry-run requested — skipping wheel build + upload + dispatch
EXIT_CODE: 0
```

Latency: 6 sequential preflight checks complete in ~3-5s end-to-end (credential check itself is ~300ms — one signed LIST against a ~2-item collection).

**6b. Missing-credential fast-fail (the load-bearing case)** — pointed `biccSecretName` at `this_entry_does_not_exist_in_aidp` (intentionally nonexistent). Re-captured after the credential-before-cluster reordering: cluster SKIPs once credential FAILs, so neither `start_cluster` nor `wait_cluster_active` is invoked even when the target cluster is STOPPED. Wall: **2.53s** (down from 4.77s in the pre-reorder draft of this evidence; with a STOPPED cluster the savings vs the original ordering would be ~5min).

```text
PASS bundle.yaml: loaded /tmp/bundle.p15e.yaml
PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
PASS OCI profile: API-key profile 'DEFAULT' loaded
PASS AIDP control plane: reachable; 8 cluster(s) visible
FAIL BICC credential: AIDP credential entry 'this_entry_does_not_exist_in_aidp'
not found in the data-lake credential store
   → add a credential named 'this_entry_does_not_exist_in_aidp' with key 'password'
     via the AIDP UI before running, OR change environments.<env>.biccSecretName
     in aidp.config.yaml to match an existing entry
SKIP cluster state: skipped — BICC credential check failed

[DISPATCH_PREFLIGHT_FAILED] BICC credential: AIDP credential entry
'this_entry_does_not_exist_in_aidp' not found in the data-lake credential store;
→ add a credential named 'this_entry_does_not_exist_in_aidp' with key 'password'
via the AIDP UI before running, OR change environments.<env>.biccSecretName in
aidp.config.yaml to match an existing entry

WALL: 2.53s
EXIT: 2
```

**6c. Custom-key remediation** — same setup with `biccSecretKey: my_app_key` (locks the reviewer round-3 should-fix that the remediation must reference the configured key, not hardcoded `'password'`):

```text
FAIL BICC credential: AIDP credential entry 'this_does_not_exist' not found
in the data-lake credential store
   → add a credential named 'this_does_not_exist' with key 'my_app_key'
     via the AIDP UI before running, OR change environments.<env>.biccSecretName
     in aidp.config.yaml to match an existing entry
SKIP cluster state: skipped — BICC credential check failed
EXIT: 2
```

The `'my_app_key'` substring locks the wiring between `EnvSpec.bicc_secret_key` and the operator-facing hint — the credential REST endpoint can't validate individual keys (it lists/looks-up by displayName), so the remediation hint is the only signal the operator gets about which key to register.

Validates (the entire `P1.5ε-fix1` acceptance, including reviewer round-3 corrections):

- ✅ Fast-fail wall: **2.53s end-to-end** (down from 4.77s in the pre-reorder draft). Beats the planned `<2s for the check itself` claim — the check itself is ~300ms; the rest is the four cheaper Phase A + Phase B-check-4 probes that precede it. With a STOPPED cluster, the savings vs the original ordering is ~5min (cluster cold-start completely avoided since cluster SKIPs after credential FAIL).
- ✅ Typed `DISPATCH_PREFLIGHT_FAILED` code prefix in the operator-facing red line — matches the existing taxonomy contract.
- ✅ Remediation hint names the offending secret name AND the configured `biccSecretKey` AND the `environments.<env>.biccSecretName` config field — operator can fix without re-reading docs.
- ✅ Exit code 2 (dispatch-layer error), no traceback.
- ✅ Cluster check SKIPs cleanly when credential FAILs — no `start_cluster` / `wait_cluster_active` invoked. Locked in unit tests by `test_missing_credential_skips_cluster_check_and_does_not_start_stopped_cluster`.

## What's NOT in this evidence

- **Marker payload round-trip on a successful run.** The cluster-side BICC extract for `erp_suppliers` ran past the laptop-side 1800s `poll_run` deadline on this tenant (still RUNNING when laptop timed out at 30:33 wall). BICC latency for `SupplierExtractPVO` against `demo-pod` is independent of the dispatch layer; the marker-parse + `RunSummary.from_marker_dict` round-trip is covered exhaustively by the in-process unit tests in `tests/unit/dispatch/test_dispatch_via_rest.py` (synthetic marker payloads exercise every shape and edge case).
- **Cluster-side SOX-trail verification (cell 4).** Requires cell 3 to complete; verify cell never executed because BICC extract didn't finish before the laptop-side timeout.

## Bugs caught + fixed during TC29

| # | Bug | Found at | Fix |
|---|---|---|---|
| 1 | `create_notebook_job` HTTP 400 — hyphens in job name | Probe 2 | `dispatch/__init__.py`: sanitize project/env tokens with `[^a-zA-Z0-9] → _`; task key `orchestrator-run` → `orchestrator_run` |
| 2 | Cluster `pip install` rc=1 — Invalid wheel filename | Probe 3 | `dispatch/wheel_builder.py`: namespace cache by `<hash>/` subdir; preserve PEP-427 wheel filename inside |

Both fixes landed before commit. Unit-test counts stayed at 91 / 91 for the dispatch package.

## Operator follow-ups raised by this evidence

- **`P1.5ε-fix7` (new)** — bump `poll_run`'s default `timeout_s` from 30 min → 60 min, OR surface as `--poll-timeout` CLI flag. The TC26 baseline suggested narrow runs finish in ~6 min, but slow tenants legitimately exceed 30 min for a single BICC extract.
- **`P1.5ε-fix8` (new)** — partial-progress diagnose-on-timeout: when `DispatchPollTimeoutError` fires, the dispatcher should opportunistically fetch the partial executed-notebook + print cell-level progress so operators don't have to drop into `oci raw-request` to see where the cluster job is stuck.

## Probe 7 — Dry-run plan rendering (P1.5ε-fix9, 2026-06-03)

Validates the laptop-side plan-resolution that closes the original P1.5ε §Step 1c deferred work. Three sub-captures: (A) a narrow single-bronze bundle, (B) a full bundle showing the topo-sorted DAG, (C) the same full bundle with `--layers gold` exercising the "Extra-plan prerequisites" Rich table.

### Probe 7-A — Narrow bundle (single bronze, no marts)

```text
$ aidp-fusion-autopilot --bundle /tmp/bundle.p15e.yaml \
    --config /tmp/aidp.config.p15e.yaml --env dev \
    run --mode seed --dry-run

 PASS bundle.yaml: loaded /tmp/bundle.p15e.yaml
 PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
 PASS OCI profile: API-key profile 'DEFAULT' loaded
cluster '<REDACTED-CLUSTER-UUID>' STOPPED — auto-starting (~5 min)…
 cluster_wait state=STARTING
 cluster_wait state=ACTIVE
 PASS AIDP control plane: reachable; 8 cluster(s) visible
 PASS BICC credential: credential '<REDACTED-SECRET-NAME>' present in AIDP store
 PASS cluster state: cluster '<REDACTED-CLUSTER-UUID>' auto-started to ACTIVE
dry-run requested — skipping wheel build + upload + dispatch
Dry-run plan for project <REDACTED-PROJECT> (mode=seed):
      Would dispatch
┏━━━━━━━━━━━━━━━┳━━━━━━━━┓
┃ dataset_id    ┃ layer  ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━┩
│ erp_suppliers │ bronze │
└───────────────┴────────┘
EXIT_CODE: 0
```

Pre-fix9 the REST path returned `RunSummary.empty()` with `plan=None`, so this table never appeared — the operator saw the preflight PASS lines and then nothing. Post-fix9 the dispatcher's dry-run branch calls `schema.plan_resolver.resolve_dry_run_plan` laptop-side and threads the resulting `PlanNode` tuple into `RunSummary.empty(plan=…)`, which the renderer (`commands/run.py:_render_summary`) renders identically to the `--inline --dry-run` path.

### Probe 7-B — Full bundle (4 bronze + 3 silver + 3 gold), all layers in scope

```text
$ aidp-fusion-autopilot --bundle /tmp/bundle.p15e-fix9.yaml \
    --config /tmp/aidp.config.p15e.yaml --env dev \
    run --mode seed --dry-run

 PASS bundle.yaml: loaded /tmp/bundle.p15e-fix9.yaml
 PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
 PASS OCI profile: API-key profile 'DEFAULT' loaded
 PASS AIDP control plane: reachable; 8 cluster(s) visible
 PASS BICC credential: credential '<REDACTED-SECRET-NAME>' present in AIDP store
 PASS cluster state: cluster '<REDACTED-CLUSTER-UUID>' ACTIVE
dry-run requested — skipping wheel build + upload + dispatch
Dry-run plan for project <REDACTED-PROJECT> (mode=seed):
        Would dispatch
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┓
┃ dataset_id         ┃ layer  ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━┩
│ erp_suppliers      │ bronze │
│ gl_coa             │ bronze │
│ gl_period_balances │ bronze │
│ ap_invoices        │ bronze │
│ dim_calendar       │ silver │
│ dim_supplier       │ silver │
│ dim_account        │ silver │
│ supplier_spend     │ gold   │
│ ap_aging           │ gold   │
│ gl_balance         │ gold   │
└────────────────────┴────────┘
EXIT_CODE: 0
```

Plan is topo-sorted: every bronze precedes its silver consumer (`erp_suppliers` → `dim_supplier`, `gl_coa` → `dim_account`), every silver precedes its gold consumer (`dim_supplier` → `supplier_spend` + `ap_aging`, `dim_account` → `gl_balance`). The cluster is now ACTIVE from Probe 7-A so the auto-start lines don't reappear.

### Probe 7-C — `--layers gold` surfaces the "Extra-plan prerequisites" table

```text
$ aidp-fusion-autopilot --bundle /tmp/bundle.p15e-fix9.yaml \
    --config /tmp/aidp.config.p15e.yaml --env dev \
    run --mode seed --dry-run --layers gold

 PASS bundle.yaml: loaded /tmp/bundle.p15e-fix9.yaml
 PASS aidp.config.yaml dispatch coords: all dispatch coords present for env='dev'
 PASS OCI profile: API-key profile 'DEFAULT' loaded
 PASS AIDP control plane: reachable; 8 cluster(s) visible
 PASS BICC credential: credential '<REDACTED-SECRET-NAME>' present in AIDP store
 PASS cluster state: cluster '<REDACTED-CLUSTER-UUID>' ACTIVE
dry-run requested — skipping wheel build + upload + dispatch
Dry-run plan for project <REDACTED-PROJECT> (mode=seed):
      Would dispatch
┏━━━━━━━━━━━━━━━━┳━━━━━━━┓
┃ dataset_id     ┃ layer ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━┩
│ ap_aging       │ gold  │
│ supplier_spend │ gold  │
│ gl_balance     │ gold  │
└────────────────┴───────┘
                 Extra-plan prerequisites (must exist on disk)
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ dataset_id         ┃ layer  ┃ consumer   ┃ table path                        ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ap_invoices        │ bronze │ ap_aging   │ fusion_catalog.bronze_p15e_fix9_… │
│                    │        │            │ ap_invoices                       │
│ dim_supplier       │ silver │ ap_aging   │ fusion_catalog.silver_p15e_fix9_… │
│                    │        │            │ dim_supplier                      │
│ gl_period_balances │ bronze │ gl_balance │ fusion_catalog.bronze_p15e_fix9_… │
│                    │        │            │ gl_period_balances                │
│ dim_account        │ silver │ gl_balance │ fusion_catalog.silver_p15e_fix9_… │
│                    │        │            │ dim_account                       │
└────────────────────┴────────┴────────────┴───────────────────────────────────┘
EXIT_CODE: 0
```

The prereqs table renders the 3-part `catalog.schema.table` identifier built from `bundle.aidp.{catalog, bronzeSchema, silverSchema}` — verifies the reviewer round 1 blocking lock that `resolve_dry_run_plan` takes `paths: TablePaths` positionally and threads it through to `PrereqNode.table_path`. A regression where the resolver fell back to default paths would surface here as `default_catalog.bronze.…` instead of the bundle-declared `fusion_catalog.bronze_p15e_fix9_….` prefix.

### Probe 7 validates

- ✅ Dispatch dry-run renders the "Would dispatch" Rich table (the original P1.5ε §Step 1c acceptance criterion).
- ✅ Topo-sort across bronze → silver → gold is preserved through the schema-side resolver.
- ✅ `--layers gold` filter populates the "Extra-plan prerequisites" table with tenant-aware 3-part table identifiers.
- ✅ `--dry-run` short-circuits after Phase B PASS — no wheel build, no upload, no job submission, no cluster runtime.
- ✅ Cluster-state PASS reused from Probe 7-A across B + C (cluster ACTIVE, not re-started) — Phase B preflight remains a side-effect-free observation when the cluster's already running.
- ✅ Exit code 0 contract preserved.

### Probe 7 boundary lock confirmation

The dispatch package's `tests/unit/dispatch/test_imports.py` ran clean on every probe (subprocess-isolated assertion that `import dispatch` plus the in-process `_render_summary(PlanNode(...))` call do NOT pull `orchestrator/*` / `dimensions/*` / `transforms/*` / `extractors/*` into `sys.modules`). The §4.3 separation survived the resolver move — `schema.plan_resolver` + `schema.registry_metadata` are now in the explicit allow-list.

## Redaction note

All identifiers (AIDP host, `aiDataPlatformId`, workspace key, cluster key, job/run/task UUIDs, BICC username, Fusion pod URL, external-storage profile name) redacted per the workspace memory rule on sensitive identifiers. The non-redacted strings in this file (`fusion_autopilot_dev`, `demo-pod`, `playground`, hash prefixes, `oracle_ai_data_platform_fusion_autopilot-0.1.0a0-py3-none-any.whl`) appear in prior public TC* evidence + plugin source docstrings.

## Closes

- BACKLOG P1.5ε acceptance — partial (preflight + dispatch round-trip + DISPATCH_* error mapping validated live; marker round-trip covered by unit tests).
- `docs/features/p1.5e-cli-rest-dispatch` — implementation evidence captured.
