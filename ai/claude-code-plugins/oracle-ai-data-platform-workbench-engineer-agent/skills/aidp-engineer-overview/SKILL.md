---
name: aidp-engineer-overview
description: Router + setup/auth troubleshooting for Oracle AI Data Platform (AIDP) data-engineering work. Use when the user mentions AIDP and isn't sure which skill applies, asks "what can you do with AIDP", describes a task by capability, or hits an AIDP auth/workspace/cluster error. Routes to the right aidp-* skill and carries the shared environment + auth-ladder rules.
---

# `aidp-engineer-overview` — router & shared environment rules

Pick the right skill for an AIDP task, and own the cross-cutting setup/auth rules every other skill relies on.

## When to use
- The user mentions AIDP but the specific operation is unclear, or names several at once.
- The user asks "how do I … in AIDP" / "what can this do".
- Any `aidp-*` skill fails with an auth, workspace, or cluster error.

## First run
If `.aidp/catalog.md` does not exist, route to **`aidp-engineer-bootstrap`** (one-time setup), then
**`aidp-catalog-init`** (grounding), before answering data questions. The plugin is self-contained — it
does not require a bootstrap of any MCP server to run.

## Routing table
| User intent | Skill |
|---|---|
| First-run setup / resolve env (region, DataLake OCID, workspace, cluster) | `aidp-engineer-bootstrap` |
| Create a DataLake instance or workspace (incl. private network) | `aidp-workspace-admin` |
| Build the catalog map / "discover my lakehouse" | `aidp-catalog-init` |
| Answer a business question / run **read** Spark SQL | `aidp-analyzing-data` |
| **Write** SQL — INSERT/UPDATE/DELETE/MERGE, CREATE/ALTER/DROP, OPTIMIZE/VACUUM/time-travel | `aidp-sql-ddl` |
| Create/alter/drop table·view·schema·catalog (control-plane); register an external catalog | `aidp-table-management` |
| Browse catalogs/schemas/tables/volumes, resolve a name→key | `aidp-catalog-explore` |
| Profile a table (nulls/distinct/min-max) | `aidp-profiling-tables` |
| Data-quality rule checks | `aidp-data-quality` |
| Recent activity / async operations | `aidp-observability` |
| Load a file into a table | `aidp-ingest-file-to-table` |
| Workspace files / notebooks CRUD | `aidp-workspace-files` |
| Volumes (PAR up/down, dirs) | `aidp-volumes` |
| Build/run/monitor a Job (pipeline, schedule) | `aidp-pipelines` |
| Author/run a notebook + kernel session | `aidp-notebooks` |
| Cluster lifecycle / libraries | `aidp-cluster-ops` |
| Debug a slow/failed Spark job | `aidp-spark-debugging` |
| Tune/optimize a slow Spark job (skew, spill, shuffle, small files, joins, AQE, Delta, JDBC reads) | `aidp-spark-optimization` |
| Define metrics / logical model | `aidp-semantic-model` |
| Register/validate reusable verified queries | `aidp-verified-queries` |
| Federate / join across multiple sources | `aidp-federate` |
| LLM inside SQL (`ai_generate`) | `aidp-ai-sql` |
| Secrets / credential store | `aidp-credentials` |
| Delta sharing (shares/recipients) | `aidp-data-sharing` |
| Git in the workspace | `aidp-git` |
| Author/deploy/run an agent flow (13 node types, guardrails) | `aidp-agent-flows` |
| Code an agent in Python (LangGraph / aidputils, high-code) | `aidp-agent-highcode` |
| Create a reusable standalone tool (SQL/Prompt/RAG/HTTP/Custom/MCP) | `aidp-tools` |
| Build a Knowledge Base / RAG / vector index | `aidp-knowledge-bases` |
| Deploy/purge a resource bundle | `aidp-bundle` |
| Roles / permissions / access (incl. per-resource grants, masking) | `aidp-roles-access` |
| MLflow experiments / models | `aidp-mlops` |
| Installed models catalog | `aidp-models-catalog` |
| Audit logs (enable/retention, search) | `aidp-audit` |
| User settings / preferences | `aidp-user-settings` |
| Databricks → AIDP migration | `aidp-migration` |

## Out of scope → point elsewhere
- **Connecting to an external source** (Oracle ADB/ExaCS, Fusion, EPM, Essbase, Snowflake, S3, Kafka, …) —
  **including a request like "a notebook that connects to Fusion"** → the
  **`oracle-ai-data-platform-workbench-spark-connectors`** plugin: use its `aidp-<source>` skill for the
  connection recipe — **do NOT hand-roll the connection.** Check it's installed (`claude plugin list`); if
  not, tell the user to install it. Its `oracle_ai_data_platform_connectors` helper package is installed once
  via that plugin's **`aidp-connectors-bootstrap`** skill (pushes it to `/Workspace/Shared` via the AIDP MCP;
  if the MCP can't reach the instance, upload manually). A single external source needs only that connector
  skill (then author/run via `aidp-notebooks`); `aidp-federate` composes several for cross-source joins.
- **OCI networking** (VCN/NAT/ACL), **OAC** dashboards/registration, **DFL/Maxwell** — not handled here.

## Shared environment & auth rules (every skill inherits these)
- **Engine precedence — self-contained, no MCP/private-repo dependency.** For every CONTROL-PLANE op
  (catalogs, schemas, tables, clusters, jobs, workspaces, roles, volumes, files, credentials, sharing,
  bundle, mlops, audit, user-settings, …), use this order — see `references/aidp-cli-map.md`:
  1. **Preferred: the official `aidp` CLI** — `aidp <group> <command> --auth api_key --profile DEFAULT
     --instance-id <DATALAKE_OCID>` (public, Oracle-supported: github.com/oracle-samples/aidataplatform-sdk;
     `aidp command-groups` / `aidp search`). Use it when installed.
  2. **Fallback: `oci raw-request`** against the same REST API
     (`https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/…`, api_key `--profile
     DEFAULT`) — see `references/oci-raw-request.md` / `references/no-mcp-rest-map.md`. Same endpoint + auth
     as the CLI, so it is a drop-in when the CLI isn't installed or doesn't expose the op (e.g. full Git,
     agent-flow authoring). Do NOT invent endpoints — use the references.
  - **Interactive Spark-SQL / notebook cells** run via the bundled helper:
    `python "$PLUGIN_DIR/scripts/aidp_sql.py" --region <r> --datalake <ocid> --workspace <ws> --cluster <key>
    --code <python/spark code>`. It mints a UPST from the api_key DEFAULT profile, auto-creates a scratch
    notebook, and returns JSON (`status` / `outputs` / `spark_job_ids`). No `AIDP_SESSION` required;
    `--session-profile` is optional. Use a `SELECT 1` cell as the trivial smoke test.
  - **The `aidp` MCP is an OPTIONAL accelerator only.** If an MCP happens to be configured you MAY use its
    tools, but it is NOT required and NOT assumed. Never frame the plugin as depending on it, and never
    route to bootstrap *to install an MCP* — there is nothing to install for the plugin to work.
- **Workspace-first.** AIDP operations are workspace-scoped. Confirm/select the workspace before acting;
  pass the workspace key/name explicitly (`--workspace` to the helper, `…/workspaces/<ws>/…` in REST URLs)
  when the default isn't the target.
- **Cluster must be RUNNING** for any data/SQL op. Check cluster status
  (`GET /workspaces/<ws>/clusters/<key>`); start it (`POST …/actions/start` with a `{}` body) if stopped.
- **Persist + confirm every mutation.** Before any create/update/delete/run/deploy/grant, write the request
  body to `.aidp/payloads/<verb>-<resource>.json`, show it to the user, and **confirm** before running it
  (auditable + re-runnable — see `references/payloads.md`). Especially for deploy/purge/delete/grant/share.
- **Auth ladder:** `--profile DEFAULT` (api_key) → on 401/403/"NotAuthenticated"/"Security Token":
  `oci session refresh --profile AIDP_SESSION` then retry with `--auth security_token --profile AIDP_SESSION`;
  if refresh fails, `oci session authenticate --profile AIDP_SESSION --region <region>`. The helper mints its
  own UPST from DEFAULT; pass `--session-profile AIDP_SESSION` only if a tenancy rejects IAD api keys
  (session-token only). Some tenancies do reject IAD API keys outright.
- **Never hardcode or print OCIDs/keys/tokens.** Never trust a local `.env` for region/OCID/profile.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) — skill → official `aidp` CLI command (preferred engine)
- [references/oci-raw-request.md](../../references/oci-raw-request.md) — control-plane REST fallback + auth
- [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) — endpoint map per skill
- [references/payloads.md](../../references/payloads.md) — `.aidp/payloads/` mutation-persistence convention
- `scripts/aidp_sql.py` — bundled interactive Spark-SQL helper
- [references/mcp-tool-map.md](../../references/mcp-tool-map.md) — optional MCP accelerator (not required)
