# Multi-MCP Chat Agent for AIDP

A natural-language chat agent for AIDP (Oracle AI Data Platform) that fans
out across **three** Oracle data sources via the Model Context Protocol:

- **Oracle Autonomous Database (ADB)** — transactional records via Select AI MCP (NL → SQL)
- **Oracle Analytics Cloud (OAC)** — subject areas / datasets via Logical SQL
- **Oracle Integration Cloud (OIC) 3** — integrations exposed as tools through OIC's project-scoped MCP server

Every server is reached over HTTPS MCP. There is no Node.js subprocess and
no local bridge — token management is fully Python-native. All three
servers authenticate via OAuth 2.0 grants:
**ADB → password grant**, **OAC → JWT Bearer (User Assertion)**, **OIC →
client_credentials**. Each can be re-authenticated on demand from cached
config, so there's no rotating refresh chain that can break overnight.

Each integration can be **independently enabled or disabled** in
[./config.yaml](./config.yaml) — you can run the agent with just ADB, just
OAC, just OIC, or any combination.

---

## Folder layout

```
multi-mcp-chat-agent/
├── README.md                ← this file
├── agent.py                 ← the AgentBasic AIDP entrypoint
├── config.sample.yaml       ← config template with inline setup notes
├── enable-mcp-selectai.sql  ← ADB-side SQL template (only if ADB enabled)
└── mint-and-exchange.py     ← OAC JWT verification helper (only if OAC enabled)
```

Use [./config.sample.yaml](./config.sample.yaml),
[./enable-mcp-selectai.sql](./enable-mcp-selectai.sql), and
[./mint-and-exchange.py](./mint-and-exchange.py) as shareable
templates — copy and customize before running.

---

## Quick start

1. Copy [./config.sample.yaml](./config.sample.yaml) to
   [./config.yaml](./config.yaml) and fill in values for any integration
   you want to enable. Set `enabled: false` on the rest.
2. (If OAC is enabled) Follow the OAC JWT setup steps below — generate a
   keypair, register the cert in IDCS, prep `oac-jwt-key.txt`. Verify the
   chain with [./mint-and-exchange.py](./mint-and-exchange.py) BEFORE
   deploying the agent.
3. Upload [./agent.py](./agent.py), [./config.yaml](./config.yaml) (your
   version of [./config.sample.yaml](./config.sample.yaml) with your
   parameters), and [./oac-jwt-key.txt](./oac-jwt-key.txt) (if OAC) to
   your AIDP project. Deploy through the AIDP agent UI (NOT the play
   button — that bypasses the OCI signer wiring needed for the LLM).
4. Send a message in the AIDP chat panel. The first message takes a few
   seconds while MCP sessions warm up; subsequent messages are fast.

---

## Per-integration setup

This guide covers **only** what's needed to expose each Oracle service over
MCP and connect this agent to it. It assumes you already have an ADB
instance with data, an OAC instance with datasets/subject areas, and an
OIC project with integrations — building those is out of scope.

### 1. LLM (always required)

OCI Generative AI is the LLM backend. The agent uses OCI's signer
principal injected by AIDP — no API key needed.

- Pick a compartment on-listed for OCI Generative AI in your region.
- Choose any chat-capable model from the catalog. `xai.grok-4-fast-non-reasoning`
  is a solid default.
- Fill in `llm.compartment_id`, `llm.region`, `llm.model_id` in
  [./config.yaml](./config.yaml).

### 2. Enable Select AI MCP on ADB

Two pieces are needed: a **tag on the ADB instance** (OCI Console) to
expose the MCP endpoint, and **SQL inside the database** to enable
resource-principal auth, create a Select AI profile, and register at
least one tool.

**Prerequisites**: ADB 19c v19.29+ or 26ai v23.26+. Older versions don't
support Select AI MCP.

**a. Enable MCP on the ADB instance (OCI Console — required first)**

Without this tag the data-access MCP endpoint isn't exposed, no matter
what you do in SQL.

- OCI Console → Autonomous Database → your ADB instance → **Tags** →
  **Add tag**:
  - **Tag namespace**: leave free-form
  - **Tag key**: `adb$feature`
  - **Tag value**: `{"name":"mcp_server","enable":true}`
- Save. Within ~1 minute the MCP endpoint at
  `https://dataaccess.adb.<region>.oraclecloudapps.com/adb/mcp/v1/databases/<adb_ocid>`
  becomes live.

**b. Create the Select AI profile and MCP tool (in-database SQL)**

Use [./enable-mcp-selectai.sql](./enable-mcp-selectai.sql)
as a starting point. Open it in SQL Developer Web (connected as `ADMIN`),
edit the four marked spots if you want to (profile name, schema owner, tool name, smoke
test question), and run each block sequentially. The template:

1. Drops any old credential (idempotent).
2. Calls `DBMS_CLOUD_ADMIN.ENABLE_RESOURCE_PRINCIPAL()` so ADB can call
   OCI Generative AI as itself — creates the `OCI$RESOURCE_PRINCIPAL`
   credential automatically.
3. Auto-discovers all tables in the schema and creates a Select AI
   profile pointing at `provider=oci` with the resource-principal
   credential.
4. Registers the profile as an MCP tool via
   `DBMS_CLOUD_AI_AGENT.CREATE_TOOL`. **The tool is what MCP exposes** —
   no tool, no MCP traffic.
5. Smoke-tests with `DBMS_CLOUD_AI.GENERATE(..., action => 'runsql')`.

**c. Wire up [./config.yaml](./config.yaml)**

Note the ADB OCID, region, and user/password the agent will use. Set
`integrations.adb.enabled` to `true` and fill in the rest.

**Auth model**: OAuth 2.0 Resource Owner Password Credentials grant
(commonly "password grant") against ADB's data-access endpoint. The
agent fetches a fresh bearer on every MCP rebuild — no token persistence
needed.

### 3. OAC MCP server (JWT User Assertion)

OAC exposes MCP at `/api/mcp` on the analytics host. The agent
authenticates via **OAuth 2.0 JWT User Assertion**: it locally signs a
short-lived JWT using a private key, exchanges it at IDCS for an OAC
access_token, and uses that bearer to call `/api/mcp`. There's no
rotating refresh chain — every access_token is freshly minted, so the
chain can't break overnight (the failure mode of the older OAC
`tokens.json` flow).

The trade-off: the JWT must assert a real OAC user as the `sub` claim,
so the agent operates as a **dedicated service user** (e.g.
`oac-agent-svc@your-org.com`). All chat users see OAC data filtered as
that service user — no per-user RLS in this sample.

> **Security note**: Setting Client Type to "Trusted" and registering a
> cert is essentially the IDCS admin authorizing the cert holder to
> impersonate any active user in the domain. This is by design (same
> model as GCP Service Accounts, AWS STS, Kerberos S4U). Treat the
> private key like a root password: never commit it, rotate annually,
> and prefer a low-privilege service user over your personal account.
> Per-user RLS *is* possible by minting per-message JWTs based on the
> chat user's identity, but that's not implemented in this sample.

#### One-time IDCS setup

**a. Generate keypair + X.509 cert** (on your laptop):

```bash
openssl genrsa -out oac-jwt-key.pem 2048
openssl req -x509 -key oac-jwt-key.pem -out oac-cert.pem \
  -days 365 -subj "/CN=oac-mcp-agent"
```

**b. IDCS Confidential Application**:

You can **reuse the OIC Confidential App** if you already created one
for OIC, OR create a separate one for OAC. Either way, on that App:

- **Allowed grant types**: ☑ **JWT Assertion** AND ☑ **Client Credentials**.
  Both are needed — Client Credentials gates the validation of our
  `client_assertion` JWT even though we're using the JWT-bearer grant.
  Without it you get a generic "system error" from IDCS.
- **Edit OAuth configuration** → set **Client type: Trusted** (not
  Confidential — Trusted is required to import a signing cert).
- **Certificate** section → click **Import certificate** → upload
  `oac-cert.pem`, give it a memorable **Alias** like `oac-mcp` (you'll
  put this same value in `config.yaml` as `cert_alias`). IDCS wants an
  X.509 cert here, not just a public key — that's why the
  `openssl req -x509` step is mandatory.
- **Token Issuance Policy → Resources**: click **Add scope** and search
  for your OAC instance. It registers under a name like
  `ANALYTICSINST_<your-host>-<region>` (search for `analyticsinst` if
  filtering by `oac` doesn't find it — Visual Builder shows up under
  `oac` but isn't what you want). Pick the scope ending in
  `urn:opc:resource:consumer::all`. Copy that full scope URL — you'll
  put it in `config.yaml`.
- **Status**: Active.

If reusing the OIC app: no new client_id/secret to generate. Just enable
the JWT grant type, switch to Trusted client type, import the cert, and
add the OAC scope.

**c. OAC service user**:

In OAC, identify or create the service user (e.g.
`oac-agent-svc@your-org.com`). Grant them the OAC permissions you want
the agent to inherit (typically read access on the relevant subject
areas / datasets).

#### Verify the chain (recommended before deploying agent.py)

Before touching agent.py / config.yaml in AIDP, confirm the IDCS+OAC
config is correct using [./mint-and-exchange.py](./mint-and-exchange.py).
It mints both JWTs, exchanges them at IDCS, and prints a curl one-liner
to test against `/api/mcp`. If both succeed, you're ready to deploy. If
they fail, fix IDCS first — don't waste cycles debugging the agent.

You can run it in **two ways** — pick whichever is easier for you:

**Option 1 — From your laptop**:

Requires Python 3.11+ with `requests` and `cryptography` installed
(`pip install requests cryptography`). Edit the CONFIG block at the top
of the script with your values, then:

```bash
cd /path/to/multi-mcp-chat-agent
python3 mint-and-exchange.py
```

**Option 2 — From inside AIDP** (if you don't want to install Python
locally, or want to test from the same network the agent will run in):

The script uses only `requests` and `cryptography`, both of which are
already in AIDP's runtime — same dependencies the agent itself relies
on. Upload `mint-and-exchange.py` to your AIDP project alongside the
key file, then use AIDP's "play / run" button on the script to execute
it. The output (token preview + curl command) will appear in AIDP's
script-output panel.

Either way: if the script prints `✓ access_token` and the curl call
returns 200 with a `tools` list, you're set. If it errors, check the
IDCS error code and the troubleshooting table at the bottom of this
README for what to fix.

#### Wire up [./config.yaml](./config.yaml)

- **Rename `oac-jwt-key.pem` → `oac-jwt-key.txt`**. Reason: AIDP's file
  picker (the dropdown when uploading files into a project) only accepts
  certain extensions — Python, JSON, TXT, CSV, PSV, SH, YAML — and
  `.pem` isn't on that list. The file content is identical PEM bytes;
  the rename is purely to make AIDP accept the upload. The agent reads
  it as raw bytes and feeds them to `cryptography.load_pem_private_key`,
  so the actual format on disk is still PEM.

  ```bash
  cp oac-jwt-key.pem oac-jwt-key.txt
  ```

- Upload `oac-jwt-key.txt` to the same AIDP project folder as
  [./agent.py](./agent.py).
- Set `integrations.oac.enabled` to `true` and fill in:
  - `url` — the OAC analytics base URL (NOT including `/api/mcp`)
  - `idcs_host` — your OCI Identity Domain host
  - `client_id` — the Confidential App client_id
  - `service_user_email` — the OAC service user's email
  - `scope` — the full scope URL you copied from Token Issuance Policy
    → Resources. Format:
    `https://<hash>.analytics.ocp.oraclecloud.comurn:opc:resource:consumer::all`
    (note: hash hostname, not the user-facing `lucastestoac-*` URL; no
    `://` separator before `urn:`).
  - `cert_alias` — the alias you set when importing the cert in IDCS
    (e.g. `oac-mcp`). The agent puts this in the JWT `kid` header so
    IDCS can find the right cert to verify the signature against.
  - `private_key_filename` — defaults to `oac-jwt-key.txt`

#### How the agent uses this at runtime

- **At deploy time** (`setup()`): stages `oac-jwt-key.txt` →
  `$HOME/oac-mcp/jwt-signing.pem`, loads the private key into memory,
  mints the first access_token by signing two JWTs and exchanging them
  at IDCS.
- **No background refresh loop, no proactive freshness check.** All
  three integrations (ADB password grant, OAC JWT assertion, OIC
  client_credentials) can be re-authenticated on demand from cached
  config + private key — there's no rotating refresh chain that needs
  scheduled tending.
- **On error mid-message**: if a tool call fails with an auth or
  network error (401/403, ClosedResourceError, etc.), the reactive
  retry path mints a fresh JWT, rebuilds MCP, heals any orphan
  tool_calls in conversation state, and tries the same message once
  more. The user sees ~1-2s of extra latency the first time they chat
  after a long idle, but never a failed message.
- **The only operational break point** is **key rotation** — yearly or
  whatever cadence you choose. To rotate: regenerate the keypair,
  register the new cert in IDCS (delete the old alias or upload
  alongside), re-upload `oac-jwt-key.txt` to AIDP, redeploy.

### 4. OIC MCP server

OIC's MCP server is per-project. Enabling it is three orthogonal pieces
that ALL must be in place — missing any one results in `403 Forbidden`,
empty tool lists, or `invalid_scope`.

**a. Enable MCP on the project**

- In OIC Designer, open the project.
- Click the **pencil / Edit** icon at the top right of the project page.
- ☑ **Enable MCP server** → **Save**.
- Re-open the edit panel — the **MCP server URL** field is now visible.
  Canonical pattern (use this if the displayed URL looks wrong):
  `https://<oic-host>/mcp-server/v1/projects/<projectId>/mcp`.

**b. Register tools via the AI Agents tab**

OIC doesn't auto-publish all integrations to MCP. You explicitly pick
which ones to expose and write a description the LLM will see.

- In the project sidebar → **AI Agents** → **Tools** → **Add**.
- The **Integration** dropdown only shows integrations that are
  *eligible*: **REST-triggered, activated, and shared with other projects**.
  A scheduled integration won't appear; an integration without
  "Share with other projects" enabled won't either.
- Pick one, give it a name, and write a clear **description** — the
  description is what the LLM uses to decide when to call the tool.
- Save. Repeat for each integration you want exposed.

**c. Confidential Application + ServiceInvoker role**

The MCP endpoint authenticates via OAuth 2.0 client_credentials. There
is **no** OCI Resource Principal, no API-key signing, no Basic Auth path
for OIC MCP — Oracle's docs are explicit on this.

- In your OCI Identity Domain (IDCS), create or reuse a **Confidential
  Application**. Allowed grant types must include **Client Credentials**.
- Under **OAuth configuration → Token issuance policy → Resources**, add
  the OIC instance application. Two scopes appear — add **both**:
  - `urn:opc:resource:consumer::all` on the instance host
  - `/ic/api/` on the same host
- Both scopes use the **IDCS-registered hash hostname** (something like
  `1CE41DD5...integration.region.ocp.oraclecloud.com`) — NOT the
  user-facing `design.*` console hostname. Copy them verbatim from the
  Resources table.
- **Activate** the application (Actions → Activate at the top of the
  app detail page).
- **Map the App to `ServiceInvoker`** on the OIC instance — this is the
  most-missed step:
  - OCI Console → Identity → Domains → your domain → **Oracle Cloud
    Services** → your OIC instance → **Application Roles** →
    **ServiceInvoker** → **Assigned applications** → assign the
    Confidential App.
  - `ServiceAdministrator` alone is **not enough** — MCP specifically
    requires `ServiceInvoker`. A token without it returns `403` from the
    MCP endpoint even though the OAuth round-trip succeeds.

**d. config.yaml**

Set `integrations.oic.enabled` to `true` and fill in `mcp_url`,
`idcs_host`, `client_id`, `client_secret`, and `scope` (the two scope
URLs space-separated).

**Auth model**: OAuth 2.0 client_credentials. Token TTL ~1 hour. No
scheduled refresh — the agent re-fetches a fresh bearer on demand,
triggered by the reactive-retry path when a tool call fails with 401.

**Sanity check** — verify the OAuth + MCP chain works locally before
deploying:

```bash
TOKEN=$(curl -s -u "<client_id>:<client_secret>" \
  -d "grant_type=client_credentials" \
  --data-urlencode "scope=<your space-separated scopes>" \
  https://<idcs_host>/oauth2/v1/token | jq -r .access_token)

curl -i -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' \
  <oic_mcp_url>
```

A 200 with a `tools` array confirms the chain is sound. A 403 typically
means the `ServiceInvoker` role mapping is missing.

---

## Concurrency

The agent serves all chat users from one Python process. Concurrent calls
to ADB MCP, OAC MCP, OIC MCP, and OCI Generative AI all run in parallel.
Conversation state is per-user via AIDP's `thread_id`.

A single `asyncio.Lock` serializes MCP rebuilds (first load and
reactive auth-error retry) to prevent races. Tool-call execution is
unaffected.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `MCP setup error: ... 401 Unauthorized` on ADB | wrong ADB user/password or OCID | check [./config.yaml](./config.yaml) values; try logging in via SQL Developer Web |
| `🔑 OAC JWT authentication failed` in chat → IDCS `invalid_grant` | service user disabled, missing, or wrong email | check `integrations.oac.service_user_email` against an Active OAC user |
| `🔑 OAC JWT authentication failed` in chat → IDCS `invalid_client` | cert not registered, or wrong `client_id` | re-upload `oac-cert.pem` as a Trusted Client cert on the Confidential App; verify `integrations.oac.client_id` |
| `🔑 OAC JWT authentication failed` in chat → IDCS `invalid_request` / signature | wrong private key, malformed JWT, or clock skew > 5 min | confirm `oac-jwt-key.txt` matches the cert in IDCS; verify the AIDP container clock is roughly correct |
| OAC works but tools return empty | service user lacks data access | grant the service user permissions on the OAC subject areas / datasets you query |
| `403 Forbidden` on OIC MCP | Confidential App not mapped to `ServiceInvoker` on the OIC instance | OCI Console → Identity → Domains → Oracle Cloud Services → your OIC → Application Roles → ServiceInvoker → assign the App |
| `invalid_scope` from IDCS | scope value uses the wrong hostname or is missing from Token issuance policy | copy scope verbatim from the App's Token issuance policy → Resources |
| `OAuth Client app is inactive` | Confidential App is in Inactive state | App detail page → Actions → Activate |
| OIC MCP returns no tools | project's "Enable MCP server" flag is off, or no tool registered under AI Agents → Tools | tick the flag, register at least one integration as a tool |
| First message after long idle is slow, subsequent ones fast | reactive retry caught a 401, minted a fresh JWT, and rebuilt MCP | expected; this is the recovery path. Look for "Recoverable error mid-invoke" log line |

---
