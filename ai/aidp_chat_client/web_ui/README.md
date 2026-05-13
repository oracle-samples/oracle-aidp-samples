# AIDP Agent Chat — Web UI

A minimal browser chat UI for a deployed AIDP agent endpoint. Demonstrates how
to put a frontend in front of an agent without exposing OCI credentials to the
browser.

## How it works

```
   browser  ──HTTP──▶  Flask proxy  ──signed HTTPS──▶  AIDP agent endpoint
   (index.html)        (server.py)                     (gateway.aidp...)
              ◀──SSE──            ◀────── SSE ───────
```

The browser cannot sign OCI requests safely, so a local Flask proxy holds your
OCI credentials, signs each call, and streams the agent's SSE response back to
the page unchanged. The proxy reuses `AIDPChatClient` from the parent library
to construct the signer (API key or security token).

## Prerequisites

- Python 3.8+
- An OCI config file (typically `~/.oci/config`)
- A deployed AIDP agent and its full chat endpoint URL — copy it straight from
  the AIDP console after deployment (looks like
  `https://gateway.aidp.<region>.oci.oraclecloud.com/agentendpoint/<id>/chat`).
  If you don't have an agent yet, [`multi-mcp-chat-agent`](../../agent-flows/code-first/multi-mcp-chat-agent/)
  in this repo is one ready-to-deploy option you can chat with.

## Run it

From this directory:

```bash
python3 -m pip install -e ..                # install the parent aidp_chat_client library
python3 -m pip install -r requirements.txt  # flask + flask-cors
```

> Use `python3 -m pip` (not bare `pip`) so the deps land in the same Python
> that runs the script — on macOS the two often resolve to different installs
> and you'll get `ModuleNotFoundError: aidp_chat_client` otherwise.

Open [server.py](server.py) and set `AGENT_URL` to the full chat endpoint URL
from the AIDP console (the constants block is at the top of the file). Then:

```bash
python3 server.py
```

Open [http://localhost:5001](http://localhost:5001).

## Configuration

All configuration lives at the top of [server.py](server.py):

| Constant | Default | Purpose |
|---|---|---|
| `AGENT_URL` | placeholder | Full chat endpoint URL from the AIDP console |
| `OCI_CONFIG_PATH` | `None` (uses `~/.oci/config`) | Path to your OCI config file |
| `OCI_PROFILE` | `"DEFAULT"` | Profile inside the config file |
| `AUTH` | `"api_key"` | `"api_key"`, `"security_token"`, or `"resource_principal"` |

Each constant also reads from an environment variable (`AIDP_AGENT_URL`,
`AIDP_AUTH`) so you can override without editing the file — which is how
`deploy.py` injects values when deploying to a Container Instance.

## Files

- `server.py` — Flask proxy. Loads OCI auth via the parent library, exposes
  `POST /chat`, streams SSE through to the browser.
- `index.html` — single-page chat UI. Generates a per-tab `session_id`, parses
  `data: {...}` SSE events, and renders agent output as markdown
  (`marked` + `DOMPurify` from CDN).
- `requirements.txt` — proxy-only deps (`oci` and `requests` come transitively
  from the parent library).
- `Dockerfile` — Python image used by `deploy.py`. Build context is the parent
  `aidp_chat_client/` directory so the library is installable inside the image.
- `deploy.py` — one-command build + push + deploy to an OCI Container Instance.
- `config-deploy.sample.yaml` — template for `deploy.py`. Copy to
  `config-deploy.yaml` and fill in your values (compartment, agent URL, etc.)
  to skip interactive prompts. The real `config-deploy.yaml` is gitignored.

## Deploy to OCI Container Instances

`deploy.py` builds the Docker image, pushes it to OCIR, sets up a VCN/subnet
with port 5001 open, and creates a Container Instance running the proxy. It
auto-discovers tenancy, region, and namespace from `~/.oci/config`, prompts
you to pick a compartment (or reads `DEPLOY_COMPARTMENT_ID` /
`config-deploy.yaml`), prompts to create an OCIR auth token if you don't
have one free, and prints the public URL when done.

```bash
python3 -m pip install oci pyyaml   # if not already installed
python3 deploy.py                    # interactive
# or:  python3 deploy.py --yes
```

#### Skip the prompts: `config-deploy.yaml`

`deploy.py` will prompt for a handful of things every run (region,
compartment, agent URL, OCIR auth token). To skip those, copy
[`config-deploy.sample.yaml`](config-deploy.sample.yaml) to
`config-deploy.yaml`, fill in the values, and re-run. The script reads it
from the same directory and uses the values directly. Anything left blank
falls back to the prompt. The OCIR auth token can also live in this file,
but the file is gitignored — never commit it.

#### Manual / bring-your-own deploy

`deploy.py` is opinionated (creates a VCN, OCIR repo, Container Instance,
IAM Dynamic Group + Policy). If you'd rather use your own infrastructure
or CI/CD pipeline, the moving parts are:

1. **Build** the Docker image. Build context must be the parent
   `aidp_chat_client/` directory so the library is installable inside
   the image:
   ```bash
   cd aidp_chat_client
   docker build -f web_ui/Dockerfile -t my-aidp-chat-ui:latest .
   ```
2. **Push** to your container registry of choice.
3. **Run** with these environment variables set:
   - `AIDP_AGENT_URL` — full chat endpoint URL
   - `AIDP_AUTH` — `resource_principal` (recommended for non-laptop
     deployments) or `api_key` if you mount an OCI config file
4. **Identity setup** — same two layers documented below
   (IAM Dynamic Group + Policy, plus AIDP Workbench membership).

### Required: identity setup for the deployed container

Inside the container, the proxy authenticates as the Container Instance itself
(an OCI resource principal). For that principal to actually invoke the AIDP
agent, **two layers of access** must be in place:

#### Layer 1 — IAM (Dynamic Group + Policy)

Grants the resource principal permission on the underlying OCI resources.

`deploy.py` checks for both at startup and refuses to deploy if they're
missing — so you'll get an immediate, clear error instead of a deploy that
"succeeds" and then fails on the first chat. The check is read-only (only
needs `inspect` on identity), so any user who can deploy can also run it.

You have two paths:

- **Manual (default)** — create them once in the OCI Console:
   1. **Dynamic Group** (in the Default identity domain) with this matching
      rule:
      ```
      ALL {resource.type = 'computecontainerinstance', resource.compartment.id = '<COMPARTMENT_OCID>'}
      ```
   2. **Policy** in your compartment:
      ```
      Allow dynamic-group <DG_NAME> to use ai-data-platforms in compartment <COMPARTMENT_NAME>
      ```
   The script prints the exact rule and statement to paste, scoped to your
   chosen compartment.

- **Auto-create** — `python3 deploy.py --create-iam`. The script tries to
  create both. Needs **tenancy-admin permissions** (Dynamic Groups live at
  the tenancy root). Falls back to printing the manual instructions if perms
  are insufficient or your tenancy uses Identity Domains (where DGs are
  managed inside the domain console rather than the classic identity API).

#### Layer 2 — AIDP Workbench membership

> **Important:** IAM alone is not enough. The AIDP gateway adds its own
> Workbench-level auth check on top of OCI IAM. Your dynamic group must also
> be added as a **member of the AIDP Workbench** that hosts the agent.
>
> Symptom when this layer is missing: the gateway returns **404 Not Found**
> on chat (not 401/403). The same URL works locally because your *user* has
> Workbench access automatically — the resource principal does not.

Steps:

1. Get the OCID of the Dynamic Group `aidp-agent-chat-ui-dg` from the OCI
   Console (Identity → Domains → Default → Dynamic Groups).
2. Open the **AIDP Workbench** that hosts your agent.
3. Go to the **Roles** tab in the left menu.
4. Add the dynamic-group OCID as an **Admin** (simplest — narrower roles
   would also work if AIDP exposes them for your use case).
5. Save.

After both layers are in place the resource principal can authenticate as
the workbench, the gateway lets it through, and the agent responds.

## Security note

The proxy authenticates to the AIDP gateway as a **single principal** —
your user (when running locally with `api_key` / `security_token`) or
the Container Instance itself (when deployed with `resource_principal`).
Whichever it is, **anyone who can reach the proxy chats as that
principal**. There's no per-user auth in front of it.

This is acceptable for:
- Local dev on your laptop (only you can reach `localhost:5001`)
- A Container Instance behind a VPN, private subnet, or IP allowlist

Before exposing this on the public internet for shared use, add user
authentication in front of the proxy (e.g. a reverse proxy with OAuth2
or basic auth) and consider per-user scoping if your downstream agent
supports it.
