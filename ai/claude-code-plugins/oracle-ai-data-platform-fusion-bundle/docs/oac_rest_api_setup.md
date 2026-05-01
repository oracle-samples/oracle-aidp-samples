# OAC REST API setup — automating `dashboard install`

The bundle's `aidp-fusion-bundle dashboard install --target oac` command runs a **hybrid install**:

1. **Connection** — generates the 6-key JSON file; admin uploads via OAC UI in ~3 minutes (`Data → Connections → Create → "Oracle AI Data Platform"`).
2. **Workbooks** — imports each `oac/workbooks/*.dva` via OAC's REST API. First run opens browser for one-time SSO consent; subsequent runs use a cached refresh token silently.

This doc covers the **one-time setup** required for step 2 (workbook REST import). If you skip this setup, the bundle still produces the connection JSON via `--print-only` — you just upload workbooks manually too.

---

## Why two flows?

OAC's REST API at `/api/20210901/catalog/*` requires **user-context Bearer tokens** ([Oracle's Authenticate doc](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/authenticate.html)):

> "You must obtain the token with a user context. This means you can't use some grant types, such as the Client Credentials grant type, to access Oracle Analytics Cloud REST APIs."

So we use the **Authorization Code + PKCE** OAuth flow: the bundle opens a browser the first time, the user signs in (federation/MFA all work), the bundle captures the token via a localhost callback, and persists a refresh token for silent reuse on subsequent runs.

Connection creation is more annoying: Oracle does **not** publish the AIDP `connectionType` discriminator for OAC's REST API — the AIDP connection in OAC is documented as UI-driven only (via [config.json upload in the AIDP Quick Start](https://blogs.oracle.com/ai-data-platform/continuing-your-oracle-ai-data-platform-journey-quick-start-guide)). So the bundle generates the JSON and walks the admin through the UI upload.

---

## Prerequisites

| | |
|---|---|
| **OAC role on the user** | The user signing in to OAuth consent must have **BI Service Administrator** application role on the OAC instance ([Predefined Application Roles](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acabi/predefined-application-roles.html)). |
| **IDCS Identity Domain access** | You need to create one Confidential Application in the OAC's IDCS domain. Identity Domain Administrator is sufficient; non-admin can be granted just "Manage applications" if your organization has a finer-grained policy. |
| **Browser** | First run opens a browser. For SSH-only / headless boxes, use `--auth-flow device` (Device Code Grant — user enters a code on any device with a browser). |

---

## One-time IDCS setup (5 min)

### Step 1 — Create a Confidential Application

OCI Console → **Identity & Security → Domains → \<your-OAC-domain\> → Integrated applications → Add application → Confidential Application**.

| Field | Value |
|---|---|
| Name | `aidp-fusion-bundle-installer` (or any name) |
| Description | `OAuth client for aidp-fusion-bundle workbook REST imports` |

Click **Next**.

### Step 2 — Configure as OAuth client

- **Client configuration**: **Configure this application as a client now**
- **Allowed grant types**: Check **Authorization code** + **Refresh token** + **Client credentials** *(client_credentials is unused by the bundle but kept for parity with future tooling)*
- **Allow non-HTTPS URLs**: **ON** (loopback callback is `http://localhost:8765/callback`)
- **Redirect URL**: `http://localhost:8765/callback` (you can change the port via `--callback-port` if 8765 is in use; redirect URL must match exactly)

### Step 3 — Add OAC as a resource

- Scroll to **Resources → Add resources**
- Pick the auto-created service app: `ANALYTICSINST_<your-oac-name>_<region>`
- The picker shows one scope: `urn:opc:resource:consumer::all`. Add it. (Don't worry about the name — despite "consumer," this is the scope OAC uses for both read and write per Oracle's Authenticate doc; permission is enforced by the user's OAC role, not by the OAuth scope.)

### Step 4 — Activate and capture credentials

Click **Save** then **Activate**. Then:

- **Client ID**: copy from the General Information panel
- **Client Secret**: click **Show secret** and copy (or store in OCI Vault)

---

## Running the install

```bash
aidp-fusion-bundle dashboard install --target oac \
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
  --client-secret '<step-4-client-secret-or-${vault:OCID}>'
```

**First run** — browser opens to IDCS, you sign in (federation/MFA all work), you see "OAC consent received. You can close this tab." — tokens are persisted to `~/.aidp-fusion-bundle/oac-token.json` (mode 0600 on POSIX).

**Subsequent runs** — silent refresh-token round-trip. No browser pop-up.

The bundle then:
1. Writes `oac/data_source/<connection_name>.json` (you upload this once via OAC UI — see "Connection upload" instructions printed to terminal)
2. Imports each `oac/workbooks/*.dva` via REST

---

## Headless / CI installs

Pass `--auth-flow device` for Device Code Grant. The bundle prints a verification URL and an 8-character code; open the URL on any device with a browser, enter the code, approve. The bundle polls until you approve, then proceeds. Refresh token persists the same way.

---

## Custom audience override

The bundle auto-discovers the IDCS audience prefix from OAC's `/ui/` redirect (probes the `idcs_app_name` query parameter). If your OAC instance is behind a non-standard load balancer / CDN that breaks the probe, pass `--oauth-scope` explicitly:

```bash
--oauth-scope 'https://<your-audience-prefix>.analytics.ocp.oraclecloud.comurn:opc:resource:consumer::all offline_access'
```

The audience prefix for OAC always lives at `https://<24-char-id>.analytics.ocp.oraclecloud.com` — different from the user-facing OAC URL. You can find it in the IDCS service-app's `audience` field via OCI CLI:

```bash
oci identity-domains apps list \
  --endpoint https://idcs-<stripe>.identity.oraclecloud.com \
  --filter 'displayName co "<your-oac-name>"' \
  --query 'data.resources[0].audience'
```

---

## Troubleshooting

### `invalid_scope` from IDCS token endpoint

The audience-prefix in your scope string doesn't match a published scope on the OAC service-app. Use `--oauth-scope` with the exact audience returned by the OCI CLI command above.

### `HTTP 500 PublicAPIJerseyServlet` on catalog calls

You're hitting OAC with a **client_credentials** token. Per Oracle, OAC requires user-context tokens. The bundle now uses Auth Code + PKCE by default; if you're seeing this, check that `--auth-flow auth_code` (the default) hasn't been overridden, and your IDCS Confidential Application has `authorization_code` + `refresh_token` grants enabled.

### `Insufficient Permissions` after SSO consent

The user you signed in as doesn't have `BI Service Administrator` role on this OAC instance. Have an OAC admin grant it via OAC Console → Service Administration → Application Roles.

### Browser doesn't open

If the bundle prints "Opening browser..." but nothing happens (corporate VM / no default browser), copy the URL printed below it and open in any browser. The flow continues once you complete consent and your browser hits `http://localhost:8765/callback`.

If you need fully-headless: switch to `--auth-flow device`.

---

## Cleaning up

If you want to revoke access:

1. **Tokens**: `rm ~/.aidp-fusion-bundle/oac-token.json` (next run will require re-consent)
2. **IDCS app**: OCI Console → Identity & Security → Domains → \<your-domain\> → Integrated applications → `aidp-fusion-bundle-installer` → Actions → Deactivate (or Delete)
3. **OAC artifacts**: `aidp-fusion-bundle dashboard uninstall --target oac --workbooks <name1>,<name2>` removes the workbooks the bundle imported (the connection has to be removed via OAC UI per the design boundary noted above)

---

## References

- [Oracle Analytics REST API — Authenticate](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/authenticate.html)
- [Oracle Analytics REST API — Quick Start](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/quick-start.html)
- [Securing OAC REST API with Device Authorization Grant — A-Team](https://www.ateam-oracle.com/securing-oracle-analytics-cloud-rest-api-with-oci-iam-oauth-20-device-authorization-grant)
- [Unlocking OAC with OAuth 2.0 — Mike Durran](https://medium.com/oracledevs/unlocking-oracle-analytics-cloud-with-oauth-2-0-e62218efb277)
- [AIDP Quick Start (UI flow with config.json)](https://blogs.oracle.com/ai-data-platform/continuing-your-oracle-ai-data-platform-journey-quick-start-guide)
- [TC10 live test results](../tests/live/TC10_oac_integration_results.md) — full evidence trail TC10a–TC10h
