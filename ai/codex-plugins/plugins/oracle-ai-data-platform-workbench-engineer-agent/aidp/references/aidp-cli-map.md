# Skill → official `aidp` CLI map (primary engine)

The plugin's **preferred control-plane engine is the official Oracle `aidp` CLI** from
[`oracle-samples/aidataplatform-sdk`](https://github.com/oracle-samples/aidataplatform-sdk) (v1.0.0,
Python/TS/Java; PyPI/npm/Maven "coming soon"). It is public, Oracle-supported, and its 215 commands across
16 groups map ~1:1 to this plugin's skills. **Precedence (every skill inherits this):**

1. **Official `aidp <group> <command>`** when the CLI is installed (preferred — supported + versioned).
2. **`oci raw-request`** against the same `aidp.<region>` REST API as the fallback when the CLI isn't present
   or doesn't expose the operation (see [`oci-raw-request.md`](oci-raw-request.md) / [`no-mcp-rest-map.md`](no-mcp-rest-map.md)).
3. **`$HOME/.aidp/aidp_sql.py`** for interactive Spark-SQL / notebook **cell execution** — the official CLI/SDK
   does **not** execute cells (its Notebook group is files + sessions only; running a notebook is job-based).

Global CLI flags: `--profile`/`-p` (default `DEFAULT`), `--auth api_key|security_token|instance_principal|resource_principal`
(CLI default `security_token`; **use `--auth api_key --profile DEFAULT`** to match this plugin's verified path),
`--region`, `--endpoint` (default `https://aidp.<region>.oci.oraclecloud.com`), `--instance-id <DataLake OCID>`.
Discover with `aidp command-groups`, `aidp search <term>`, `aidp help <group>`.

> **Endpoint (LIVE-VERIFIED 2026-06-09):** the `aidp` CLI's default `--endpoint`
> `https://aidp.<region>.oci.oraclecloud.com` is the gateway that works (the CLI ran end-to-end on tpcds).
> The **Python SDK** defaults to a *different* host (`datahub-dp.<region>.oci.<sld>` + `/20260430/aiDataPlatforms/`)
> which **404s** on a tenancy not on the GA host — so when calling the SDK directly set
> `AIDP_ENDPOINT=https://aidp.<region>.oci.oraclecloud.com`. That gateway serves both the GA
> `20260430/aiDataPlatforms` (CLI/SDK) and the legacy `20240831/dataLakes` (our `oci raw-request` fallback).

## Mapping

| Skill | Official `aidp` CLI commands (primary) | SQL helper / REST fallback |
|---|---|---|
| `aidp-catalog-init` / `aidp-catalog-explore` | `catalog list\|get\|create\|update\|delete\|refresh\|test-connection` · `schema list\|get\|list-tables\|get-table\|list-views\|get-view` | REST `GET /catalogs`,`/schemas?catalogKey=`,`/tables?catalogKey=&schemaKey=` |
| `aidp-ingest-file-to-table` | `schema generate-temp-file-upload-target` → `schema infer`/`infer-with-preview` → `schema create-data-table`/`create-table` (also `retrieve-par`) | REST upload/infer/create |
| `aidp-analyzing-data` / `profiling-tables` / `data-quality` / `ai-sql` / `federate` / `verified-queries` | (no CLI cell-exec) — use **`$HOME/.aidp/aidp_sql.py`** for `spark.sql(...)` / `ai_generate(...)` | `$HOME/.aidp/aidp_sql.py` |
| `aidp-sql-ddl` | (no CLI cell-exec) — DDL/DML + Delta maintenance via **`$HOME/.aidp/aidp_sql.py`** (CREATE/INSERT/UPDATE/DELETE/MERGE/OPTIMIZE/VACUUM/time-travel — live-verified) | `$HOME/.aidp/aidp_sql.py` |
| `aidp-table-management` | `catalog create\|update\|delete\|refresh\|test-connection` · `schema create-table\|update-table\|delete-table\|refresh-table\|create-view\|create\|delete` | REST `…/catalogs`,`/schemas`,`/tables`,`/views` (+ SQL DDL via `aidp-sql-ddl`) |
| `aidp-notebooks` | files: `notebook create-content\|get-content\|update-content\|modify-content\|delete-content\|export-contents`; sessions: `notebook create-session\|get-session\|list-sessions\|patch-session\|delete-session` | cell exec → `$HOME/.aidp/aidp_sql.py` |
| `aidp-workspace-files` | `workspace-object create\|get\|head\|list\|update\|copy\|move\|rename\|delete\|upload-with-par\|download-with-par` | REST notebook contents API |
| `aidp-volumes` | `volume list\|get\|create\|update\|delete\|list-files\|make-dir\|update-dir\|delete-dir\|upload-file[-with-par]\|download-file[-with-par]\|delete-file` | REST `/volumes?catalogKey=&schemaKey=` |
| `aidp-pipelines` | `workflow create-job\|update-job\|get-job\|list-jobs\|delete-job\|create-job-run\|get-job-run\|list-job-runs\|list-recent-job-runs\|cancel-job-run[s]\|repair-job-run\|list-task-runs\|get-task-run\|fetch-output\|export-task-run-output` | REST `/workspaces/{ws}/jobs…` |
| `aidp-cluster-ops` | `cluster list\|get\|get-default\|create\|update\|delete\|start\|stop\|restart\|list-libraries\|patch-library\|download-logs\|search-logs\|summarize-metrics-data` | REST `…/clusters…` (start/stop need `{}` body) |
| `aidp-spark-debugging` | `cluster search-logs\|download-logs\|summarize-metrics-data` | Spark-UI via `$HOME/.aidp/aidp_sql.py` (kernel-side `spark.sparkContext.uiWebUrl + /api/v1/...`) |
| `aidp-credentials` | `credentials list\|get\|create\|update\|delete` | REST `/credentials` (Preview) |
| `aidp-data-sharing` | `delta-share create\|get\|list\|update\|delete\|manage-access\|manage-data-asset\|manage-permission\|list-data-assets\|list-permissions` + `create-recipient\|get-recipient\|list-recipients\|update-recipient\|delete-recipient\|manage-recipient-permission\|list-recipient-*` | REST `/shares`,`/recipients` (LIVE-VERIFIED 200) |
| `aidp-roles-access` | `role list\|get\|create\|update\|delete\|add-member\|remove-member\|list-permissions` · per-resource `*-permissions` / `manage-permission` (catalog/schema/**table**/**view**/cluster/volume/workspace/workspace-object) · **Job/Workflow** grants via `workflow list-job-permissions \| manage-job-permission <ws> <JOB-KEY> --body` (body = `{assignees:{type,targets},permissions:[…]}`, **not** the generic `{principals,permission,action}` — confirm enum live; CLI README `workflow manage-job-permission` + SDK `assign_job_permission_details.py`) · masking = restricted views (no masking API — probed 404) | REST `/roles…` (LIVE-VERIFIED 200) · job grants `…/workspaces/{ws}/jobs/{key}/permissions` |
| `aidp-mlops` / `aidp-models-catalog` | `mlops *` (experiments, runs, metrics/params/tags, registered-models, model-versions, artifacts, `transition-model-version-stage`, `create-workspace-model-version`) | REST `/mlops/api/2.0/mlflow/*` (Preview) |
| `aidp-bundle` | `bundle create\|deploy\|fetch-deployment-status\|purge\|sync-bundle` | REST `/bundles…` (Preview; may be unprovisioned) |
| `aidp-workspace-admin` | `workspace create\|get\|list\|update\|delete\|create-git-folder\|list-permissions\|manage-permission\|list-create-permissions\|manage-create-permission` (instance create is OCI control-plane, not data-plane) | REST `/workspaces` |
| `aidp-observability` | `async-operations list\|get` · `workflow list-recent-job-runs` | REST `/asyncOperations` |
| `aidp-audit` *(new)* | `audit manage-logs\|search-logs` | REST `/audit…` |
| `aidp-user-settings` *(new)* | `user-setting list\|get\|create\|update\|delete` | REST `/userSettings…` |
| `aidp-git` | only `workspace create-git-folder` exists in CLI v1.0.0; full branch/commit/merge GitService is **not in the CLI** → REST (Preview, may be unprovisioned) | REST `…/gitRepositories…` |
| `aidp-agent-flows` | **no CLI group in v1.0.0** (Python SDK has `agent_flow` models) → REST (LA), **workspace-scoped** `…/workspaces/{ws}/agentFlows`; 13 node types (`references/agent-flow-nodes.md`); guardrails lake-scoped `…/agentFlowGuardrails` (LIVE 200) | REST `…/workspaces/{ws}/agentFlows…` |
| `aidp-tools` | **no CLI group** → REST `…/workspaces/{ws}/tools` (LIVE round-trip: POST 200 / DELETE 204); toolType SQL\|PROMPT\|RAG\|HTTP\|CUSTOM\|MCP | REST `…/workspaces/{ws}/tools` |
| `aidp-knowledge-bases` | **no CLI group** → REST `…/knowledgeBases?catalogKey=&schemaKey=` (lake-scoped, LIVE 400=exists); KB jobs for ingest; HNSW/IVF index | REST `…/knowledgeBases…` |
| `aidp-agent-highcode` | composition — high-code Python (`aidputils` + LangGraph); authored via `aidp-workspace-files`, runs on AI Compute — no direct CLI surface | — |
| `aidp-semantic-model` / `aidp-verified-queries` / `aidp-migration` | composition / `.aidp/*.md` files — no direct CLI surface | — |

## Notes
- **Beyond the CLI (keep as REST/Preview/LA):** full native Git (GitService), agent-flow authoring/deploy.
  Note these as "not yet in the official `aidp` CLI v1.0.0" when used.
- **Models:** the official CLI folds model registry into `mlops` (registered-models/model-versions); our
  `aidp-models-catalog` should reference `aidp mlops list-registered-models` etc.
- The CLI uses the **same data-plane endpoint + auth** as our `oci raw-request` calls, so switching engines
  is a presentation change, not a capability change. Distribution is the upgrade: the CLI is public + soon
  on PyPI/npm/Maven, replacing the private `ai-data-engineer-agent` dependency entirely.
