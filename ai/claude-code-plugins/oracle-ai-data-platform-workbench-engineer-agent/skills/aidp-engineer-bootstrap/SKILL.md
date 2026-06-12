---
name: aidp-engineer-bootstrap
description: First-run setup for the self-contained AIDP plugin. Verifies the oci CLI + a DEFAULT api_key profile, installs the bundled Python deps, discovers region + DataLake OCID + workspace, then smoke-tests both engines (a control-plane GET /catalogs via oci raw-request and a trivial SELECT 1 cell via scripts/aidp_sql.py). Use on first run, when the user asks to "set up / configure / install / bootstrap AIDP", or when control-plane or SQL calls fail with auth/config errors. No MCP install required.
---

# `aidp-engineer-bootstrap` — get a new terminal to a working agent

Take a brand-new terminal to a working AIDP agent. The plugin is **fully self-contained** — no dependency on
the private `ai-data-engineer-agent` repo or any MCP server. Engine precedence (see `references/aidp-cli-map.md`):
- **Control-plane — preferred: the official `aidp` CLI** (public, Oracle-supported:
  github.com/oracle-samples/aidataplatform-sdk). Maps 1:1 to the skills; install it if you can (Step 2b).
- **Control-plane — fallback: `oci raw-request`** against the same REST API (works with only the oci CLI;
  `references/oci-raw-request.md`, `references/no-mcp-rest-map.md`). Same endpoint + auth as the CLI.
- **Interactive Spark-SQL / notebook cells** → the bundled helper `python "$PLUGIN_DIR/scripts/aidp_sql.py"` (mints a UPST
  from the api_key DEFAULT profile, auto-creates a scratch notebook, runs the cell — the CLI/SDK can't exec cells).

## When to use
- First run, or "set up / configure / install / bootstrap AIDP", or control-plane/SQL calls failing on
  auth or missing config.

## Step 1 — Verify the oci CLI + a DEFAULT api_key profile
```bash
oci --version                                   # CLI present?
oci setup repair-file-permissions --file ~/.oci/config 2>/dev/null
oci iam region list --profile DEFAULT >/dev/null && echo "DEFAULT profile OK" || echo "fix DEFAULT profile"
```
- If `oci` is missing → install the OCI CLI (`pip install oci-cli` or the platform installer).
- The `DEFAULT` profile can be **either** an api_key profile (tenancy, user, fingerprint, key_file, region)
  **or** an `oci session authenticate` **session-token** profile — both engines have full parity (see
  "Session-token auth" in `references/oci-raw-request.md`). For api_key the WebSocket mints a UPST; for a
  session profile the session token is reused directly (no api_key needed anywhere).
- **No api_key? Use a session token everywhere:** `oci session authenticate --profile <P> --region <r>`, then
  control-plane via `--auth security_token --profile <P>`, and `scripts/aidp_sql.py --profile <P>` for cells
  (it auto-detects the session token). `--session-profile` stays as an explicit WebSocket-only override.
  Session tokens expire ~1h → `oci session refresh --profile <P>`.

## Step 2 — Install the bundled Python deps
The only code in this plugin is `scripts/aidp_sql.py`; it needs `oci`, `requests`, `websocket-client`,
`cryptography`.

> **Resolve `$PLUGIN_DIR` first** — after `claude plugin install` the plugin lives under Claude's plugins
> dir, **not** your project cwd, so the helper must be called by absolute path. Set it once:
> `export PLUGIN_DIR="${CLAUDE_PLUGIN_ROOT}"` (Claude sets `CLAUDE_PLUGIN_ROOT` for this plugin), or run
> `claude plugin list`, copy the install path, and `export PLUGIN_DIR=<that path>`. Every `aidp_sql.py`
> example in these skills uses `"$PLUGIN_DIR/scripts/aidp_sql.py"`. (On a clean first session the SessionStart
> hook already auto-installs the deps; the manual step below is only a fallback.)

```bash
python -m pip install -r "$PLUGIN_DIR/scripts/requirements.txt"
```

## Step 2b — (Preferred) install the official `aidp` CLI
The supported control-plane engine. Once it's on PyPI/npm this is one command; until then install from the
GitHub release:
```bash
# When published (coming soon):  python -m pip install aidp-cli      # provides the `aidp` command
# Today, from the v1.0.0 release zip:
#   download aidp-py-cli-1.0.0.zip from
#   https://github.com/oracle-samples/aidataplatform-sdk/releases/tag/v1.0.0 then:
#   python -m pip install ./aidp-py-cli-1.0.0.zip
aidp version && aidp command-groups        # verify the CLI is on PATH
```
**Install in a venv** — installing the SDK/CLI alongside `oci-cli` triggers pip dependency conflicts
(it downgrades `cryptography` and clashes `click`/`oci` pins; the CLI still runs, but a venv keeps `oci-cli`
and the bundled helper's deps clean). Verified live: `aidp catalog list --instance-id <OCID> --auth api_key`
returns the catalogs on `tpcds`.

If `aidp` isn't installed, that's fine — every skill falls back to `oci raw-request` (Step 4 fallback). The
plugin works either way; the CLI is just the supported, versioned path.

## Step 3 — Gather region + DataLake OCID + workspace ("check my AIDP CLI setup")
Resolve these values and make them available to the CLI. The official demo's **"Check my AIDP CLI setup"**
= confirming the instance + endpoint are set in the shell context:
```bash
export AIDP_INSTANCE_ID="<DATALAKE_OCID>"
export AIDP_ENDPOINT="https://aidp.<region>.oci.oraclecloud.com"      # or: aidp configure  (sets default profile + instance)
```
> **Always set `AIDP_ENDPOINT` to the `aidp.<region>` gateway** (LIVE-VERIFIED). The Python SDK otherwise
> defaults to `datahub-dp.<region>…/20260430/aiDataPlatforms/`, which **404s** on tenancies not on that GA
> host (the `aidp` CLI already defaults to the working gateway).
If `AIDP_INSTANCE_ID` is missing, the setup check stops there — set it (and the endpoint), then re-check.
Never hardcode these into committed files.
- **Region:** default `us-ashburn-1` (confirm with the user).
- **DataLake OCID:** ask the user for it (from the AIDP console URL/details). Do not guess one.
- **Workspace:** once you have region + OCID, **auto-discover** workspaces and let the user pick (use the
  `data[].id` / `data[].key` of the desired workspace):
  ```bash
  oci raw-request --http-method GET --profile DEFAULT \
    --target-uri "https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/workspaces"
  ```
  If the user already knows the workspace OCID, use it directly. If the GET returns 401/403, run the auth
  ladder in `references/oci-raw-request.md` (`oci session refresh --profile AIDP_SESSION`, then retry with
  `--auth security_token --profile AIDP_SESSION`).

## Step 4 — Smoke-test BOTH engines
1. **Control-plane** — confirm auth + OCID resolve. **Preferred (official CLI):**
   ```bash
   aidp catalog list --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <region>
   ```
   **Fallback (no CLI installed) — `oci raw-request`:**
   ```bash
   oci raw-request --http-method GET --profile DEFAULT \
     --target-uri "https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/catalogs"
   ```
   Expect the catalog list / `"status": 200`. (404 → wrong version/prefix; 401/403 → auth ladder.)
2. **Interactive SQL** (`scripts/aidp_sql.py`) — pick an ACTIVE cluster
   (`GET /workspaces/<ws>/clusters`), then run a trivial cell:
   ```bash
   python "$PLUGIN_DIR/scripts/aidp_sql.py" \
     --region <region> --datalake <DATALAKE_OCID> --workspace <ws> --cluster <cluster-key> \
     --code "spark.sql('SELECT 1').show()"
   ```
   Expect JSON with `"status": "ok"` and the `SELECT 1` output. (The helper mints a UPST from the DEFAULT
   profile and auto-creates `Shared/_aidp_sql_scratch.ipynb`; add `--session-profile AIDP_SESSION` only if
   the api_key path can't mint a token.)

## Step 5 — Hand off
Both engines green → hand off to **`aidp-catalog-init`** to build `.aidp/catalog.md`, then the user is ready
(`aidp-engineer-overview` for the tour, `aidp-analyzing-data` to ask questions).

## References
- [references/oci-raw-request.md](../../references/oci-raw-request.md) — base URL, auth ladder, invocation shapes
- [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) — verified control-plane endpoints per skill
- [scripts/aidp_sql.py](../../scripts/aidp_sql.py) · [scripts/requirements.txt](../../scripts/requirements.txt) — the SQL engine

<!-- Optional accelerator: if an `aidp`-style MCP server happens to be configured (`claude mcp list` shows
it Connected), you MAY use its tools as a convenience — but it is NOT required and the plugin never assumes
it. Everything above works with only the oci CLI + a DEFAULT api_key profile. -->
