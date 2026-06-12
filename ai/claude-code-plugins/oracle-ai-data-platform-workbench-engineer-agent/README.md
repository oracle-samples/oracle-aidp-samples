# Oracle AI Data Platform — Workbench Engineer Agent

> **Run your entire AI data platform in English** — one agent for the whole Oracle AI Data Platform
> Workbench: discover, query, build, govern, and ship guarded AI, all in natural language.

> **Canonical home:** [`oracle-samples/oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-engineer-agent`](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/claude-code-plugins/oracle-ai-data-platform-workbench-engineer-agent).
> End users install via Anthropic's community marketplace (see [Install](#install)), which sources from this
> canonical Oracle-org location.

Operate the entire Oracle AI Data Platform (AIDP) Workbench in natural language — a **37-skill** agent
(not a single-engine orchestrator). It discovers your catalog into a grounding cache (FK/join hints +
per-column value dictionaries), turns plain English into accurate Spark SQL, runs the full lakehouse SQL
lifecycle (CREATE/INSERT/UPDATE/DELETE/MERGE/OPTIMIZE/VACUUM/DESCRIBE HISTORY/time-travel), ingests files,
profiles data and sets quality rules, authors and repairs cron pipelines, provisions clusters
(Compute/AI Compute), and debugs jobs through the Spark UI — then keeps going where orchestrators
stop: governing the platform (roles + per-resource permissions, credential store, Delta Sharing, audit logs,
MLOps/MLflow) and shipping AI (Agent Flows across 13 node types **with guardrails**, Knowledge Base RAG,
high-code LangGraph/aidputils agents, reusable Tools). A semantic model + verified-query repository are
matched before free generation for accuracy. **Signature differentiators:** LLM-in-SQL via
`ai_generate('openai.gpt-5.4', '<prompt>')`, and cross-source federation in one Spark session.

**Engine precedence** (see [references/aidp-cli-map.md](./references/aidp-cli-map.md)): control-plane
operations prefer the official AIDP CLI when installed and fall back to `oci raw-request` against the same
AIDP REST API; interactive Spark-SQL / notebook cells run via the bundled `scripts/aidp_sql.py` helper.

> **Status:** **v0.5.0** — 37 skills across the AIDP data-engineering lifecycle (api_key **or** session-token auth). Endpoint + verification log:
> [references/rest-endpoint-map.md](./references/rest-endpoint-map.md); change history: [CHANGELOG](./CHANGELOG.md).

> **Additive to your Oracle stack** — complementary to Oracle FDI / OAC / OTBI / BIP, not a replacement.

## What it does

37 skills across the AIDP data-engineering lifecycle (each maps to an official AIDP CLI command group —
see [references/aidp-cli-map.md](./references/aidp-cli-map.md)):

| Area | Skills |
|---|---|
| **Foundation & setup** | `aidp-engineer-overview` (router), `aidp-engineer-bootstrap`, `aidp-workspace-admin`, `aidp-catalog-init` |
| **Discovery & analysis** | `aidp-analyzing-data`, `aidp-catalog-explore` |
| **Quality & observability** | `aidp-profiling-tables`, `aidp-data-quality`, `aidp-observability` |
| **Ingestion, tables & SQL** | `aidp-ingest-file-to-table`, `aidp-sql-ddl` (DDL/DML + Delta maintenance), `aidp-table-management` (catalog/schema/table/view lifecycle + external catalogs), `aidp-workspace-files`, `aidp-volumes` |
| **Pipelines & orchestration** | `aidp-pipelines`, `aidp-notebooks` |
| **Debugging & compute** | `aidp-cluster-ops`, `aidp-spark-debugging`, `aidp-spark-optimization` |
| **Reliability & semantics** | `aidp-semantic-model`, `aidp-verified-queries` |
| **Signature differentiators** | `aidp-federate`, `aidp-ai-sql` |
| **Agentic & AI** | `aidp-agent-flows` (+ all 13 node types, guardrails), `aidp-agent-highcode` (LangGraph/aidputils), `aidp-tools` (reusable tools), `aidp-knowledge-bases` (RAG) |
| **Governance** | `aidp-credentials`, `aidp-data-sharing`, `aidp-git`, `aidp-bundle`, `aidp-roles-access` (+ per-resource permissions, masking), `aidp-mlops`, `aidp-models-catalog`, `aidp-audit`, `aidp-user-settings` |
| **Migration** | `aidp-migration` |

---

## Install

> **Prerequisites (3) — no MCP required:**
> 1. The [`oci` CLI](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/cliconcepts.htm)
>    configured with a **`DEFAULT` profile — either an api_key profile *or* an `oci session authenticate`
>    session-token profile** (both are first-class). The one profile signs `oci raw-request` control-plane
>    calls and drives `scripts/aidp_sql.py`: an api_key profile mints a short-lived UPST for the Spark
>    WebSocket; a session-token profile reuses its token directly (no mint). `aidp_sql.py --session-profile`
>    can still override the WebSocket token explicitly.
> 2. **Python 3.x** — the helper deps (`oci`, `requests`, `websocket-client`, `cryptography`; no `aidp_agent`)
>    **auto-install on your first session** via the bundled SessionStart hook. No manual `pip` step needed; if
>    the readiness banner reports a dep still missing, install it from the plugin dir (`claude plugin list` →
>    `python -m pip install -r scripts/requirements.txt`, path relative to the plugin root, not your cwd).
> 3. That's it. There is **no AIDP MCP to install or register.** An MCP is an optional accelerator only.

From Anthropic's community plugin marketplace (recommended — sources from the canonical Oracle-org home):

```bash
claude plugin marketplace add anthropics/claude-plugins-community
claude plugin install  oracle-ai-data-platform-workbench-engineer-agent
```
> Helper deps auto-install on first session (SessionStart hook) — no manual `pip` needed.

Then run the one-time bootstrap and catalog discovery:

```
/aidp-engineer-bootstrap     # detects OCI DEFAULT profile + DataLake/workspace, verifies oci ✓ helper ✓ cluster ✓
/aidp-catalog-init           # one-time catalog discovery → .aidp/catalog.md grounding file
```

Now ask in natural language — e.g. *"profile store_sales", "what are the top 10 items by net sales?",
"build a daily job that refreshes the supplier-spend mart", "share the gold schema with a recipient"*.

### Running interactive Spark-SQL (the bundled helper)

Control-plane skills need no extra setup — they shell out to `oci raw-request`. Interactive Spark-SQL
and notebook cells use the bundled helper, which speaks the Jupyter v5.3 WebSocket protocol that plain
HTTP `oci raw-request` can't:

```bash
python "$PLUGIN_DIR/scripts/aidp_sql.py" \
  --region us-ashburn-1 --datalake <DATALAKE_OCID> --workspace <WS_ID> \
  --cluster <CLUSTER_KEY> --code "spark.sql('SELECT 1').show()" \
  [--profile DEFAULT] [--session-profile AIDP_SESSION] [--timeout 180]
```

It auto-creates a scratch notebook and authenticates per `--profile`: an **api_key** profile mints a
short-lived UPST; a **session-token** profile reuses its token directly (no mint). It prints JSON:
`{"status", "execution_count", "outputs", "spark_job_ids", ...}`. Exit code `0` on success, `1` on cell error.

### Optional MCP accelerator

If an AIDP MCP server is already configured in your Claude Code setup, skills may opportunistically use
its tools — but the plugin never assumes one exists and works fully without it. Nothing in install or
bootstrap registers an MCP.

---

## How it works

### Layered architecture — who calls what
```
DATA ENGINEER → natural language in Claude Code
        │
        ▼
PLUGIN: oracle-ai-data-platform-workbench-engineer-agent (37 skills)
  ┌ aidp-engineer-overview (ROUTER) — routes by intent ─────────────┐
  │  discovery │ analysis │ quality │ pipelines │ governance │ …     │
  └─────────────────────────────────────────────────────────────────┘
  GROUNDING CACHE .aidp/  : catalog.md (tables/cols/FKs/value-dicts) ·
                            semantic.md (logical names/metrics/joins) ·
                            verified-queries.md (validated Q→SQL pairs)
        │  EXECUTION RULE: SQL → aidp_sql.py ; everything else → oci raw-request
   ┌────┴───────────────┐                 ┌───────────────────────────┐
   ▼                    │                 ▼                           │
 oci raw-request        │            scripts/aidp_sql.py              │
 (CONTROL PLANE, REST): │            (INTERACTIVE SPARK-SQL):         │
 catalogs · schemas ·   │            spark.sql(...) / notebook cells  │
 tables · clusters ·    │            over Jupyter v5.3 WebSocket;     │
 jobs · volumes ·       │            api_key→UPST or session reused │
 files · roles ·        │            → JSON {status,outputs,          │
 credentials · sharing ·│               spark_job_ids,…}              │
 git · bundle · mlops ·  │                                            │
 models · agent-flows    │                                            │
 (Preview/LA flagged)    │                                            │
   └─────────┬──────────┘                 └─────────────┬─────────────┘
             │  OCI auth: DEFAULT profile — api_key (→UPST) or session-token (reused directly)
             └───────────────────────┬───────────────────┘
                                     ▼
   ORACLE AI DATA PLATFORM REST API  (20240831 · dataLakes · <DATALAKE_OCID>):
   Spark cluster · catalogs/tables · Jobs · Delta Sharing · Git · Bundles ·
   MLOps · Agent Flows

   [ optional accelerator: an AIDP MCP server, if one is already configured ]
```

### One-time setup (install + 2 commands)
```
claude plugin marketplace add anthropics/claude-plugins-community
claude plugin install  oracle-ai-data-platform-workbench-engineer-agent
   │
   ▼ first session: SessionStart hook auto-installs helper deps (oci/requests/websocket-client/cryptography)
   ▼ /aidp-engineer-bootstrap  → reads ~/.oci/config (DEFAULT), lists DataLakes/workspaces;
   │                             verifies oci ✓ aidp_sql.py ✓ cluster ✓
   ▼ /aidp-catalog-init        → writes .aidp/catalog.md (one-time grounding)
   ▼ READY → ask in natural language
```

### Per-request runtime (core loop + reliability)
```
NL request → [ROUTER] classify intent → select skill
   │
   ├─ data question (NL→SQL)?
   │     └─ match verified-queries.md?  yes→reuse validated SQL
   │        no→ground from catalog.md + semantic.md (names/FKs/value-dicts)
   │           → python "$PLUGIN_DIR/scripts/aidp_sql.py" --cluster … --code "spark.sql(…)"
   │             (api_key→UPST or session-token reused; auto scratch notebook)
   │           → show result → cache new verified pair/mappings
   │
   └─ control-plane op (catalog/clusters/jobs/ingest/volumes/files/
         credentials/sharing/git/bundle/agent-flows/roles/mlops/models)?
              → oci raw-request (20240831 · dataLakes · <DATALAKE_OCID>)
              └─ auth ladder: DEFAULT(api_key) ─401/403→ refresh AIDP_SESSION
                 → retry w/ security_token ; flag Preview/LA status
   │
   └─ on failure → inline troubleshooting: workspace-first, refresh token,
                   ensure cluster running, verify version/prefix (20240831/dataLakes)
```

---

## Design principles

- **Grounding-first** — match a *validated* question→SQL pair (verified-query repository) and ground in the
  semantic model + value dictionaries **before** free SQL generation. This is the accuracy lever that
  curated NL-to-SQL systems rely on.
- **One engine, many sources** — `aidp-federate` reads heterogeneous sources (via the spark-connectors
  plugin) into one Spark session and joins them.
- **`ai_generate()` in SQL** — LLM calls inline in Spark SQL (`aidp-ai-sql`).
- **Self-contained two-engine model** — control-plane ops via `oci raw-request`, interactive Spark-SQL via
  the bundled `scripts/aidp_sql.py` helper. No AIDP MCP or `ai-data-engineer-agent` repo required; an MCP,
  if present, is an optional accelerator only.
- **No fabrication** — Preview/LA endpoints, the `ai_generate` signature, and federation semantics are
  flagged for live verification; nothing is asserted as confirmed without a recorded live result.

## License
[MIT](./LICENSE) © 2026 Oracle Corporation
