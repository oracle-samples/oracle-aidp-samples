# AGENTS.md — Oracle AI Data Platform (AIDP) Engineer Agent (Codex)

Global context for working with **Oracle AI Data Platform** from OpenAI Codex CLI. Place this file at
`~/.codex/AGENTS.md` (global) or your project root. The 37 `aidp-*` skills under `~/.codex/skills/` (or
`~/.agents/skills/`) load natively — when a task matches a skill's description, follow that skill.

> **Self-contained.** This bundle needs only the **OCI CLI + a `DEFAULT` api_key profile + Python** — no MCP
> server required. An AIDP MCP is an optional accelerator only (see `config/codex-config.toml.snippet`).

## Your AIDP environment — fill these in (do NOT commit real OCIDs)
Set as shell env vars, or replace the placeholders when a skill asks. Skills pass them to the helper/CLI.

| Variable | Example | Meaning |
|---|---|---|
| `AIDP_REGION` | `us-ashburn-1` | OCI region |
| `AIDP_DATALAKE` | `ocid1.aidataplatform.oc1.iad.<...>` | DataLake (instance) OCID |
| `AIDP_WORKSPACE` | `<workspace-uuid>` | Workspace id/key |
| `AIDP_CLUSTER` | `<cluster-key>` | Spark cluster key (must be RUNNING for data/SQL) |

```bash
export AIDP_REGION=us-ashburn-1
export AIDP_DATALAKE=<your-datalake-ocid>
export AIDP_WORKSPACE=<your-workspace-id>
export AIDP_CLUSTER=<your-cluster-key>
```

## The bundled helper
Interactive Spark SQL / notebook cells run through the bundled helper at **`$HOME/.aidp/aidp_sql.py`**
(it mints a UPST from the api_key `DEFAULT` profile, auto-creates a scratch notebook, returns JSON). Invoke
it via Codex's `shell` tool:

```bash
python "$HOME/.aidp/aidp_sql.py" \
  --region "$AIDP_REGION" --datalake "$AIDP_DATALAKE" --workspace "$AIDP_WORKSPACE" --cluster "$AIDP_CLUSTER" \
  --code "spark.sql('SELECT 1').show()"
```
If you installed the helper elsewhere, `export AIDP_HOME=<dir>` and use `$AIDP_HOME` instead of `$HOME/.aidp`.

## Engine precedence (control-plane ops: catalogs, clusters, jobs, roles, …)
1. **Preferred — official `aidp` CLI**: `aidp <group> <command> --instance-id "$AIDP_DATALAKE" --auth api_key --profile DEFAULT --region "$AIDP_REGION"` (github.com/oracle-samples/aidataplatform-sdk).
2. **Fallback — `oci raw-request`** against the same REST API: `https://aidp.$AIDP_REGION.oci.oraclecloud.com/20240831/dataLakes/$AIDP_DATALAKE/…` (see `$HOME/.aidp/references/oci-raw-request.md`). Do NOT invent endpoints — use the references.
3. **Interactive SQL / cells** → the helper above.

## Auth ladder
`--profile DEFAULT` (api_key) → on `401/403` / "NotAuthenticated" / "Security Token":
`oci session refresh --profile AIDP_SESSION` then retry with `--auth security_token --profile AIDP_SESSION`;
if refresh fails, `oci session authenticate --profile AIDP_SESSION --region "$AIDP_REGION"`. The helper mints
its own UPST from `DEFAULT`; pass `--session-profile AIDP_SESSION` only if a tenancy rejects IAD api keys.

## Rules every skill inherits
- **Workspace-first** — AIDP ops are workspace-scoped; pass the workspace explicitly.
- **Cluster must be RUNNING** for any data/SQL op — check status, start it if stopped (`aidp-cluster-ops`).
- **Persist + confirm every mutation** — before any create/update/delete/run/deploy/grant, write the request
  body to `.aidp/payloads/<verb>-<resource>.json`, show it, and confirm.
- **Never fabricate** endpoints, OCIDs, model names, or capabilities — cite a reference, the CLI, or a live
  result. **Never hardcode or print OCIDs/keys/tokens**; never trust a local `.env` for region/OCID/profile.

## Where to start
- "What can you do with AIDP?" / unsure which skill → **`aidp-engineer-overview`** (router).
- First run → **`aidp-engineer-bootstrap`** (verify auth + deps), then **`aidp-catalog-init`** (grounding).
- Connecting to an external source (Fusion/EPM/ADB/Snowflake/S3/…) → the **AIDP Spark Connectors** bundle.
