# OAC REST API setup — automating `dashboard install`

This is the **legacy `.bar` snapshot deployment path**. The preferred current
dashboard workflow is MCP-native: `oac-dataset-advisor` recommends the OAC
dataset, the user creates the AIDP connection and dataset in OAC UI, and
`workbook-authoring` saves workbook JSON via OAC MCP. See
[../workflow.md](../workflow.md) for that flow.

The bundle's `aidp-fusion-autopilot dashboard install --target oac` command is built on **only Oracle-documented public REST endpoints** (audit done 2026-05-01 against [openapi.json](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/openapi.json)):

```
1. GET  /api/20210901/catalog?type=connections&search=<name>  (precheck — skip POST if found)
2. POST /api/20210901/catalog/connections                     (create AIDP connection — see "Realistic flow" below)
3. POST /api/20210901/snapshots                               (register customer-uploaded .bar)
4. POST /api/20210901/system/actions/restoreSnapshot          (async restore)
5. GET  /api/20210901/workRequests/{id}                       (poll until SUCCEEDED)
```

The bundle ships:
- An IDCS confidential-app config recipe (this doc, one-time)
- The 6-key AIDP connection JSON template (auto-generated)
- A `bundle-vN.bar` file as a release artifact (workbook content)

Customers upload the `.bar` to their own OCI Object Storage bucket once. The
bundle then runs the four REST calls above to install the snapshot content.

## Realistic deployment flow (live-validated TC10h-4, 2026-05-03)

OAC's REST validator on `POST /catalog/connections` does not yet bless the AIDP `idljdbc` connectionType (it falls through to generic Oracle DB schemas requiring `serviceName`/`password`/`connectionString`). The bundle therefore uses this flow on first install:

1. **(One-time, OAC UI)** Customer creates the AIDP connection via OAC UI: Data → Connections → Create → "Oracle AI Data Platform" → upload the 6-key JSON written by `aidp-fusion-autopilot dashboard install --print-only` plus the API-key PEM. This is the path Oracle documents in the [AIDP-from-OAC Quick Start blog](https://blogs.oracle.com/ai-data-platform/continuing-your-oracle-ai-data-platform-journey-quick-start-guide).
2. **All subsequent runs** of `dashboard install` are pure REST: the precheck (`GET /catalog?type=connections&search=<name>`) finds the existing connection and skips the `POST /catalog/connections` step entirely; snapshot register + restore + poll then deploy the `.bar`.

The bundle's `--overwrite-connection` flag still triggers the POST and would hit the validator gap; use `--print-only` + UI re-upload if you need to recreate the connection.

The same first-connection limitation is why the current MCP-native workflow also
asks the user to create the AIDP connection manually in OAC UI. After the
connection exists, OAC REST and MCP flows can reuse OAC-governed catalog
objects by name or dataset reference.

---

## Why snapshots and not `.dva` imports?

The audit found `POST /catalog/workbooks/imports` is **not in Oracle's openapi.json** — it's a UI-internal endpoint with no API stability guarantee. Snapshots are Oracle's only documented programmatic mechanism for moving workbook content across OAC instances ([Take Snapshots](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acabi/take-snapshots-and-restore-information.html)).

The bundle's `.bar` is built once on a clean dev OAC by the bundle author with these Custom-snapshot options:
- **Include**: Catalog Content + Shared Folders (the 5 workbooks under `/shared/AIDP_Fusion_Bundle/`)
- **Exclude**: Credentials, Connections, User Folders (so workbooks reference connection-by-name; no per-customer secrets in the .bar)

Customer's connection is created separately via `POST /catalog/connections` (step 1). On restore, OAC's `Replace Snapshot Content Only` mode adds the bundle's workbooks alongside the customer's existing content without overwriting it.

---

## Prerequisites

| | |
|---|---|
| **OAC role** | The user signing the OAuth consent must hold **BI Service Administrator** application role on the OAC instance (per [Predefined Application Roles](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acabi/predefined-application-roles.html)). Required for both connection POST and snapshot ops. |
| **IDCS Identity Domain access** | Need to create one Confidential Application in the OAC's IDCS domain. Identity Domain Administrator suffices; finer-grained "Manage applications" works too. |
| **OCI Object Storage bucket** | A bucket in the customer's OCI tenancy where they'll upload the bundle's `.bar`. Snapshot register + restore reads from there. |
| **OCI Resource Principal** for OAC | The customer's OAC instance must be granted Resource Principal access to read from the Object Storage bucket. See [Snapshot REST API Prerequisites](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/prerequisites.html). |
| **Browser** | First run opens a browser for SSO consent. Headless boxes can use `--auth-flow device`. |

---

## One-time IDCS setup (5 min)

### Step 1 — Create a Confidential Application

OCI Console → **Identity & Security → Domains → \<your-OAC-domain\> → Integrated applications → Add application → Confidential Application**.

| Field | Value |
|---|---|
| Name | `aidp-fusion-autopilot-installer` (or any name) |
| Description | `OAuth client for aidp-fusion-autopilot install` |

### Step 2 — Configure as OAuth client

- **Client configuration**: **Configure this application as a client now**
- **Allowed grant types**: **Authorization code** + **Refresh token**. Optional: **Device Code Grant** for headless setups.
- **Allow non-HTTPS URLs**: **ON** (loopback callback is `http://localhost:8765/callback`)
- **Redirect URL**: `http://localhost:8765/callback`

### Step 3 — Add OAC as a resource

- **Resources → Add resources** → pick `ANALYTICSINST_<your-oac-name>_<region>` (or `ANALYTICSAPP_<...>` on legacy IDCS-only domains)
- Add the only published scope: `urn:opc:resource:consumer::all`
- Despite "consumer" in the name, this is the scope OAC uses for both reads and writes per [Authenticate](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/authenticate.html); permission is enforced by the user's OAC role, not by OAuth scope.

### Step 4 — Activate + capture credentials

Save → Activate. Then copy:
- **Client ID** from General Information
- **Client Secret** (Show secret) — store in OCI Vault, env var, or 1Password

---

## One-time OCI Object Storage setup

### Step 5 — Create the bucket

OCI Console → **Object Storage → Buckets → Create**.

| Field | Value |
|---|---|
| Compartment | (any compartment your OAC instance can read from) |
| Name | e.g. `aidp-fusion-autopilot-bar` |
| Visibility | Private |

### Step 6 — Upload the bundle's `.bar`

Download `bundle-vN.bar` from the bundle's release artifacts (or build it yourself with `bundle build-bar` — see "Bundle author workflow" below).

Upload via OCI Console (Bucket → Upload Object) or `oci os object put`. Use a **folder-prefixed object name** — `--bar-uri` later passes the Oracle-documented `file:///<folder>/<name>.bar` shape, which is the only URI variant OAC's snapshot machinery accepts (verified live TC10h-3, 2026-05-03):

```bash
oci os object put --bucket-name aidp-fusion-autopilot-bar \
                  --file ./bundle-v0.1.0a0.bar \
                  --name aidp-fusion-autopilot/bundle-v0.1.0a0.bar
# Object name above maps to --bar-uri 'file:///aidp-fusion-autopilot/bundle-v0.1.0a0.bar'
```

### Step 7 — Grant OAC's Resource Principal access to the bucket

Add an OCI IAM policy ([Snapshot REST API Prerequisites](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/prerequisites.html)):
```
Allow any-user to read objects in compartment <your-compartment>
where ALL { request.principal.type='analyticsinstance',
            request.principal.id='<your-OAC-instance-OCID>',
            target.bucket.name='aidp-fusion-autopilot-bar' }
```

---

## Running the install

```bash
aidp-fusion-autopilot dashboard install --target oac \
  --oac-url https://<your-oac>.analytics.ocp.oraclecloud.com \
  --connection-name aidp_fusion_jdbc \
  --region us-ashburn-1 \
  --user-ocid ocid1.user.oc1..<your-ocid> \
  --tenancy-ocid ocid1.tenancy.oc1..<your-tenancy> \
  --fingerprint <api-key-fingerprint> \
  --idl-ocid ocid1.aidataplatform.oc1.iad.<your-idl> \
  --cluster-key <your-cluster-key> \
  --private-key-pem ~/.oci/oac_api_key.pem \
  --idcs-url https://idcs-<stripe>.identity.oraclecloud.com \
  --client-id <step-4-client-id> \
  --client-secret '<step-4-client-secret>' \
  --bar-bucket aidp-fusion-autopilot-bar \
  --bar-uri 'file:///aidp-fusion-autopilot/bundle-v0.1.0a0.bar' \
  --bar-password '<bar-password-if-protected>' \
  --snapshot-name aidp-fusion-autopilot-v0.1.0a0
```

**First run** — browser opens to IDCS, sign in with your OAC admin account (federation/MFA all work), see "OAC consent received." Tokens persist to `~/.aidp-fusion-autopilot/oac-token.json` (mode 0600 on POSIX).

**Subsequent runs** — silent refresh-token round-trip; no browser pop-up.

The bundle then:
1. **GETs** `/catalog?type=connections&search=<connection-name>` to find an existing connection (the realistic-flow precheck — see top of this doc)
2. If no existing connection, **POSTs** the 6-key AIDP connection JSON (subject to the `idljdbc` validator gap; if it 400s, fall back to `--print-only` + OAC UI upload)
3. **Registers** the snapshot from your Object Storage bucket (async, returns `workRequestId`)
4. **Restores** the snapshot (async, returns `oa-work-request-id` header)
5. **Polls** the work request until `SUCCEEDED`

---

## Connection-only install (skip workbooks)

Omit `--bar-bucket` and `--bar-uri` — the bundle creates the connection only. Useful when:
- You're running `dashboard install` purely to validate the auth model.
- You want to install the connection now and restore the snapshot later via OAC Console.

```bash
aidp-fusion-autopilot dashboard install --target oac \
  --oac-url ... --connection-name ... \
  ...all-connection-args... \
  --idcs-url ... --client-id ... --client-secret ...
  # no --bar-bucket / --bar-uri
```

---

## Print-only mode (no IDCS app required)

When the customer has no IDCS confidential app or doesn't want the bundle making any REST calls:

```bash
aidp-fusion-autopilot dashboard install --target oac \
  --oac-url ... --connection-name ... ... \
  --print-only
```

The bundle writes `oac/data_source/<connection-name>.json` and prints OAC UI upload instructions. The admin uploads via Data → Connections → Create → "Oracle AI Data Platform" (3-min step). Workbooks are restored via OAC Console → Snapshots → Restore.

---

## Headless / CI installs

Pass `--auth-flow device` for OAuth Device Code Grant. Bundle prints a verification URL + 8-char code; open the URL on any device with a browser, enter the code, approve. Refresh token persists the same way.

---

## Audience auto-discovery

The bundle's IDCS audience prefix usually differs from the user-facing OAC URL (e.g. `https://exampleoacprefix0123456789abcdef.analytics.ocp.oraclecloud.com` vs. `https://oac.example.com`). The bundle auto-discovers it by probing `<oac-url>/ui/` for the IDCS authorize redirect's `idcs_app_name` query parameter.

If the probe fails (non-standard load balancer, CDN, etc.), pass `--oauth-scope` explicitly:

```bash
--oauth-scope 'https://<your-audience-prefix>.analytics.ocp.oraclecloud.comurn:opc:resource:consumer::all offline_access'
```

Find the audience by running:
```bash
oci identity-domains apps list \
  --endpoint https://idcs-<stripe>.identity.oraclecloud.com \
  --filter 'displayName co "<your-oac-name>"' \
  --query 'data.resources[0].audience'
```

---

## Bundle author workflow (releasing a new bundle version)

To produce a new `bundle-vN.bar` for release:

1. On a clean dev OAC instance (compartment dedicated to bundle authoring), build the 5 workbooks under `/shared/AIDP_Fusion_Bundle/` against an `aidp_fusion_jdbc` connection pointing at your dev AIDP environment.
2. OAC Console → **Snapshots → Take Snapshot** → **Custom**:
   - **Include**: Catalog Content, Shared Folders, Application Roles (only roles the bundle relies on)
   - **Exclude**: Credentials, Connections, User Folders, File-based Data, Day by Day, Jobs, Plug-ins, Configuration
   - Set a strong password (committed in your release notes for customer use)
3. Export to OCI Object Storage in your bundle-author tenancy.
4. Download the `.bar` and upload it as a release artifact (GitHub release attachment, etc.).
5. Bump the version in `.codex-plugin/plugin.json` and `bundle.yaml`.

---

## Troubleshooting

### `invalid_scope` from IDCS token endpoint

Audience prefix in your scope doesn't match a published scope on the OAC service-app. Use `--oauth-scope` with the exact audience returned by the OCI CLI command above.

### `HTTP 401` `{"user":"...","status":"unauthorized"}` on a REST call

The user that signed in via SSO doesn't have **BI Service Administrator** role on this OAC instance. Have the OAC admin grant it via OAC Console → Service Administration → Application Roles. Then re-run with `--prompt-login` to force a fresh IDCS sign-in.

### `HTTP 400 connection schema invalid` on `POST /catalog/connections`

The AIDP `idljdbc` connectionType is reverse-engineered from the OAC UI's create-connection traffic and is **not in Oracle's published 11 connectionType samples**. OAC's REST validator therefore falls through to generic Oracle DB schemas requiring `serviceName`/`password`/`connectionString`. **This is the expected first-install path:** create the connection once via the OAC UI (re-run with `--print-only` to write the JSON, then upload via Data → Connections → Create → "Oracle AI Data Platform"). All subsequent `dashboard install` runs reuse the existing connection via the precheck — pure REST.

### `Invalid Snapshot BAR URI` on `POST /snapshots`

The URI shape must be exactly `file:///<folder>/<name>.bar`. Common mistakes:
- `bundle-v0.1.0a0.bar` (bare object name) — wrong, fails validation.
- `oci://<bucket>@<namespace>/<object>` — wrong; OAC's snapshot machinery does not accept the OCI-native URI scheme here.
- `https://objectstorage.<region>.oraclecloud.com/n/<ns>/b/<bucket>/o/<object>` — wrong; pre-authenticated request URLs aren't accepted either.
- The bundle expects you to upload with a folder-prefixed name (e.g. `--name aidp-fusion-autopilot/bundle-v0.1.0a0.bar`) and pass `--bar-uri 'file:///aidp-fusion-autopilot/bundle-v0.1.0a0.bar'`.

### `Can't load models - Restore your models` warning on OAC home page

This warning shows up on the OAC home page's Datasets row alongside a "Subject Area" icon. It refers to OAC's **classic semantic model** layer (RPD-based subject areas), not the modern self-service Datasets-from-Connections layer that this bundle uses. On a fresh OAC instance with no classic RPD uploaded, the warning is **cosmetic** — modern workbooks built on Datasets-from-Connections (which is the bundle's path) work fine despite it. The warning only matters if you also need classic Subject Areas (e.g. for OBIEE/OAC-Classic migration). The "Restore your models" action points to Console → Snapshots to restore a previous instance state that had a working classic model. Verified on OAC1 during TC10h-5 (2026-05-03): warning was present throughout, did not affect dataset-creation, workbook-authoring, snapshot capture/restore, or the bundle's `dashboard install` end-to-end success. Sources: [Oracle Analytics community thread](https://community.oracle.com/products/oracleanalytics/discussion/25759/cant-load-models-restore-your-models), [About Snapshots docs](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acabi/snapshots.html).

### `find_connection` returns `None` even though the connection exists

Old client versions called `GET /catalog?type=connections` without a `search` parameter, which OAC answers with a single-element TypeInfo header (`[{"type":"connections"}]`) — not the actual list. Fixed in `52ce2c7` (TC10h-3): `list_connections` now defaults `search="*"`. Upgrade the bundle if you see this on a build older than 2026-05-03.

### Snapshot restore fails with `ACCESS_DENIED`

OAC's Resource Principal can't read from the Object Storage bucket. Verify the IAM policy at Step 7 lists the correct OAC instance OCID and bucket name.

### Snapshot restore `SUCCEEDED` but workbooks aren't visible

The `.bar` was built with a different folder layout than the bundle expects, OR `Replace Snapshot Content Only` collided with existing customer content. Check OAC Console → Catalog → `/shared/AIDP_Fusion_Bundle/` for the workbooks; if they're missing, contact the bundle author to verify the `.bar` was built correctly.

### Browser doesn't open

Copy the URL printed below "Opening browser..." and paste into any browser. Or switch to `--auth-flow device`.

---

## Cleaning up

| Artifact | Removal |
|---|---|
| Cached refresh token | `rm ~/.aidp-fusion-autopilot/oac-token.json` |
| IDCS confidential app | OCI Console → Identity & Security → Domains → \<domain\> → Integrated applications → `aidp-fusion-autopilot-installer` → Deactivate → Delete |
| OAC connection | `aidp-fusion-autopilot dashboard uninstall --target oac --connection-name aidp_fusion_jdbc ...` |
| Registered snapshot | Add `--snapshot-id <id>` to the uninstall command (deregisters the snapshot record; restored content is not auto-rolled-back) |
| Restored workbooks | OAC Console → Catalog → delete `/shared/AIDP_Fusion_Bundle/`, OR restore an earlier snapshot of the OAC instance (take one BEFORE installing if you want a clean rollback) |

---

## References

- [Oracle Analytics REST API — Authenticate](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/authenticate.html)
- [REST endpoints index](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/rest-endpoints.html)
- [OpenAPI spec (canonical machine-readable surface)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/openapi.json)
- [Create a Connection (POST)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/op-20210901-catalog-connections-post.html)
- [Snapshot REST API Prerequisites (Object Storage + Resource Principal setup)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/prerequisites.html)
- [Take Snapshots and Restore Information (admin guide)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acabi/take-snapshots-and-restore-information.html)
- [Options When You Take a Snapshot (Custom mode content-type filters)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acmgp/options-when-you-take-snapshot.html)
- [Options When You Restore a Snapshot](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acmgp/options-when-you-restore-snapshot.html)
- [Predefined OAC Application Roles](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acabi/predefined-application-roles.html)
- [AIDP-from-OAC Quick Start blog](https://blogs.oracle.com/ai-data-platform/continuing-your-oracle-ai-data-platform-journey-quick-start-guide) — Oracle's documented UI flow for creating the AIDP connection in OAC
