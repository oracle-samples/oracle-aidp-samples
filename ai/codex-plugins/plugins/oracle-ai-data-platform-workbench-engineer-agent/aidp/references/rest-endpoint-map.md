# AIDP REST endpoint map (for the `oci raw-request` skills)

Source: memory `aidp_rest_api.md` + the AIDP platform reference, **now corrected by a live probe**.

> **LIVE-VERIFIED 2026-06-09** (tenancy `<TENANCY>` ns `<NAMESPACE>`, region `us-ashburn-1`, DataLake
> `…oc1.iad.<DATALAKE_OCID_REDACTED>…`, `oci raw-request --profile DEFAULT` api_key). **This environment serves
> `API_VERSION=20240831` and `PATH_PREFIX=dataLakes`.** GA `20260430` returns **404 here** (not live in this
> tenancy), and the `aiDataPlatforms` prefix also 404s — so default to **`20240831/dataLakes`** for this env
> and treat `20260430` as the future GA target, not a fallback to try first here. **Official-doc
> confirmation (reviewed 2026-06-11):** Oracle's GA REST reference
> ([aiwap/rest-endpoints.html](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/rest-endpoints.html),
> linked from [aidug/use-apis-sdk-cli.html](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/use-apis-sdk-cli.html))
> documents the GA surface as `/20260430/aiDataPlatforms/<id>/…` — **version-first, NO `/api/` segment**, and
> the `20260430` version pairs with the `aiDataPlatforms` prefix (the two move together; never `20260430`+`dataLakes`).
> Same path shape as the LA `20240831/dataLakes` we use, so a future LA→GA migration is a two-token swap, not a rewrite.
>
> **RE-VERIFIED 2026-06-10** on a *second, independent* instance — tpcds DataLake
> `…oc1.iad.<DATALAKE_OCID_REDACTED>…` (ws `<WORKSPACE_ID_REDACTED>`) + `aidp_skilltest` (for Spark SQL). All GA categories,
> catalog/schema/table CRUD, MCP/REST `list_roles` parity, the Spark-SQL `SELECT` path, and
> `ai_generate('openai.gpt-5.4', …)` confirmed **live**; only the Preview buckets (git/bundle/mlops) returned
> 404 = not provisioned on that instance (expected cross-instance difference, not a regression). See the
> dated 2026-06-10 tpcds block in the verification log.

Path shape: `https://aidp.<region>.oci.oraclecloud.com/<API_VERSION>/<PATH_PREFIX>/<dataLakeOcid>/…`

| Skill | Category | Status | Working endpoint (live) | API_VERSION/PREFIX | Live status (2026-06-09) |
|---|---|---|---|---|---|
| `aidp-data-sharing` | DeltaShare | GA | `GET /shares` · `GET /recipients` (+ `{k}`/actions) | 20240831 / dataLakes | ✅ **200** (shares & recipients, items=0) |
| `aidp-roles-access` | Role | GA | `GET /roles` · `…/roles/{k}/actions/addMember|removeMember` | 20240831 / dataLakes | ✅ **200** (AI_DATA_PLATFORM_ADMIN, AUDITOR); matches MCP `list_roles` |
| `aidp-models-catalog` | Models | LA | `GET /models?modelType=<GENERATIVE_AI|BASE|EMBEDDING|LLM>` | 20240831 / dataLakes | ✅ **200** (route needs `modelType`; items=0 — none installed) |
| `aidp-credentials` | credentialStore | Preview | `…/credentials` (route exists) | 20240831 / dataLakes | ⚠️ **400** route exists, GET list-shape TBD (CannotParseRequest) |
| `aidp-git` | GitService | Preview | `…/workspaces/{ws}/gitRepositories` | 20240831 / dataLakes | ❌ **404** NotAuthorizedOrNotFound — not provisioned in this env (or path differs) |
| `aidp-bundle` | Bundle | Preview | `…/workspaces/{ws}/bundles` | 20240831 / dataLakes | ❌ **404** NotAuthorizedOrNotFound — not provisioned in this env |
| `aidp-agent-flows` | AgentFlows | LA | REST `…/workspaces/{ws}/agentFlows` (**workspace-scoped**); deploy via `…/workspaces/{ws}/actions/deployAgentFlow`; guardrails lake-scoped `…/agentFlowGuardrails`. MCP tools: `MCP_TOOL` node (`toolConfig` = endpoint/auth/allowedTools/customHeaders) | 20240831 / dataLakes | ✅ **200** at workspace-scoped path (2026-06-10, collection reachable, items=0). Lake-level `…/agentFlows` **404** = wrong path, not missing provisioning |
| `aidp-mlops` | MLOps (MLflow) | Preview | `…/workspaces/{ws}/mlops/api/2.0/mlflow/…` | TBD | ❌ **404** `experiments/search` (Preview, not provisioned here / POST-shaped) |
| `aidp-knowledge-bases` | KnowledgeBases | LA | `…/knowledgeBases?catalogKey=&schemaKey=` (**lake-scoped**); create body `{displayName,description,catalogKey,schemaKey,workspaceKey,clusterKey,type,modality,embeddingModelSourceType,embeddingModelName,chunkSize,chunkOverlap,sourceFilePattern,indexDetails}`; ingest = KB jobs `{displayName,type,goal,sources,sourceKey,schedule}` | 20240831 / dataLakes | ⚠️ **400 InvalidParameter** (route exists, lake-scoped; needs real catalog/schema *keys*; ws-scoped 404) |
| `aidp-tools` | Tool | LA | `…/workspaces/{ws}/tools` (**workspace-scoped**); `POST {displayName,description,toolType,properties,inputSchema,toolConfig}`; `DELETE …/tools/{key}`; toolType ∈ SQL\|PROMPT\|RAG\|HTTP\|CUSTOM\|MCP\|NL_TO_SQL | 20240831 / dataLakes | ✅ **VERIFIED round-trip 2026-06-10**: POST→200 (returns key+toolConfig), DELETE→204. Name must start with letter, only `_` special |
| `aidp-agent-flows` (guardrails) | AgentFlows | LA | `…/agentFlowGuardrails` (**lake-scoped**); SafetyPolicy `{policyType,policyName,policyDescription,scope,action,threshold}` | 20240831 / dataLakes | ✅ **200** (2026-06-10). policyType ∈ CONTENT_MODERATION/PROMPT_ATTACKS_PREVENTION/PII_DETECTION/DENIED_TOPICS/WORD_FILTERS/CONTEXTUAL_GROUNDING/CUSTOM_POLICY; scope ∈ USER_REQUEST/AGENT_RESPONSE/BOTH; action ∈ BLOCK/INFORM/MASK |
| `aidp-pipelines` (repair) | Workflow | GA | `POST …/workspaces/{ws}/jobRuns/{jobRunKey}/actions/repair` (body `RepairJobRunDetails`) | 20240831 / dataLakes | ◻ path from SDK `workflow_client.repair_job_run`; not write-probed |
| catalog/schema/table/view CRUD | Catalog/Schema | GA | `…/catalogs`, `…/schemas?catalogKey=`, `…/tables?catalogKey=&schemaKey=`, `…/views?…` | 20240831 / dataLakes | ✅ catalogs/schemas **200**; tables/views **400 InvalidParameter** with a **bare** schemaKey — `schemaKey` **must be fully-qualified** `<catalog>.<schema>` (live: `schemaKey=default` → 400; `schemaKey=default.default` → **200** with items e.g. `default.default.web_site`). Re-verified tpcds 2026-06-10. |
| `aidp-volumes` | Volume | GA | `…/volumes` (list) · `…/volumes/{k}` · PAR up/down · mkdir | 20240831 / dataLakes | ⚠️ plain `GET …/volumes` → **400 Bad request** (tpcds 2026-06-10, via MCP `list_volumes`, deterministic 2× retry) — route reachable but rejects the bare list; likely needs params (same route-exists-but-400 pattern as `/credentials`,`/tables`). **NOT removed** — re-probe param shape when volume listing is needed. |
| masking / classification / ontologies | — | n/a | `…/maskingPolicies`,`/dataClassifications`,`/columnMaskingPolicies`,`/tags`,`/ontologies` | 20240831 / dataLakes | ❌ **all 404** — NO data-plane API in this tenancy (UI-only / not provisioned). Do NOT author a REST surface for these |

## Primary path = `oci raw-request` (MCP optional)

The plugin is self-contained: catalog, notebooks (file ops), jobs, clusters, volumes, tables, roles, and
all governance categories run via `oci raw-request` against the endpoints above — **no MCP required**.
Interactive Spark-SQL runs via the bundled `scripts/aidp_sql.py` (see `no-mcp-rest-map.md`). If an `aidp`
MCP happens to be configured, its tools mirror these endpoints and may be used as an optional accelerator
(`mcp-tool-map.md`) — but the plugin never assumes one exists.

## Verification log

```
2026-06-09  env: tenancy <TENANCY> (ns <NAMESPACE>), region us-ashburn-1, DataLake …oc1.iad.<DATALAKE_OCID_REDACTED>…
            auth: oci raw-request --profile DEFAULT (api_key) — works for IAD REST (same tenancy, cross-region)
  GET …/20240831/dataLakes/<OCID>/workspaces                  → 200  (control-plane sanity)
  GET …/20240831/dataLakes/<OCID>/shares                      → 200  items=0      [data-sharing ✅]
  GET …/20240831/dataLakes/<OCID>/recipients                  → 200  items=0      [data-sharing ✅]
  GET …/20240831/dataLakes/<OCID>/roles                       → 200  2 roles      [roles-access ✅]
  GET …/20240831/dataLakes/<OCID>/models?modelType=GENERATIVE_AI → 200 items=0    [models-catalog ✅]
  GET …/20240831/dataLakes/<OCID>/credentials                 → 400  route exists, list-shape TBD   [credentials ⚠️]
  GET …/20240831/dataLakes/<OCID>/workspaces/<WS>/gitRepositories → 404 NotAuthorizedOrNotFound      [git ❌ not provisioned]
  GET …/20240831/dataLakes/<OCID>/workspaces/<WS>/bundles     → 404 NotAuthorizedOrNotFound          [bundle ❌ not provisioned]
  GET …/20240831/dataLakes/<OCID>/agentFlows                  → 404 NotAuthorizedOrNotFound          [agent-flows REST ❌; MCP read ✅]
  GET …/20260430/… (all)                                      → 404  (GA 20260430 NOT live in this env)
  GET …/aiDataPlatforms/… (all)                               → 404  (prefix is dataLakes here, not aiDataPlatforms)

2026-06-10  env: a fresh test instance (region us-ashburn-1), <OCID> / workspace <WS>; oci raw-request --profile DEFAULT (api_key)
  GET …/20240831/dataLakes/<OCID>/workspaces/<WS>/agentFlows  → 200  items=0   [agent-flows ✅ workspace-scoped]
  GET …/20240831/dataLakes/<OCID>/agentFlows                  → 404  (lake-level path does not exist — use workspace-scoped)
  GET …/20240831/dataLakes/<OCID>/{mcp,mcpServers,remoteMcp}  → 404  (AIDP hosts NO MCP server; see below)
  GET …/20260430/aiDataPlatforms/<OCID>/{mcp,…}               → 404  (same)
  -> AIDP "remote MCP" = Native MCP CLIENT Support: an MCP_TOOL node INSIDE a flow connects out to a remote
     MCP server (OAC/ADW/OIC). There is no AIDP-hosted /mcp endpoint. See skills/aidp-agent-flows (MCP section).

2026-06-10  CAPABILITY-AUDIT probe sweep (same env; gap-closure grounding)
  GET  …/workspaces/<WS>/tools                                → 200  [standalone Tools ✅ ws-scoped]
  POST …/workspaces/<WS>/tools  {displayName,description,toolType:CUSTOM,properties:{}} → 200  returns key+toolConfig
  DELETE …/workspaces/<WS>/tools/<key>                        → 204  (round-trip VERIFIED + cleaned; name must start w/ letter, only _ )
  GET  …/agentFlowGuardrails                                  → 200  [guardrails ✅ lake-scoped]
  GET  …/knowledgeBases                                       → 400 InvalidParameter (route exists, lake-scoped, needs catalog/schema KEYS)
  GET  …/workspaces/<WS>/knowledgeBases                       → 404  (KB is lake-scoped, not ws)
  GET  …/catalogs                                             → 200 ; …/schemas?catalogKey=default → 200
  GET  …/tables?catalogKey=default&schemaKey=default          → 400 InvalidParameter (route exists, needs real schema key)
  GET  …/views?…                                              → 400 InvalidParameter (route exists)
  GET  …/{maskingPolicies,dataClassifications,columnMaskingPolicies,tags} → 404 (NO masking/classification data-plane API)
  GET  …/{ontologies, workspaces/<WS>/ontologies}            → 404 (Ontologies NOT programmatic here — UI-only)
  GET  …/userSettings                                         → 200 ; …/credentials → 400 (route exists) ; mlflow experiments/search → 404 (Preview)
  repair: POST …/workspaces/<WS>/jobRuns/<runKey>/actions/repair (RepairJobRunDetails) — path from SDK workflow_client, not write-probed
  SQL write grammar (via scripts/aidp_sql.py on USER cluster): CREATE/INSERT/UPDATE/DELETE/MERGE/OPTIMIZE/
     VACUUM/DESCRIBE HISTORY/VERSION AS OF/DROP all un-wrapped → status:ok, error:None [aidp-sql-ddl ✅ VERIFIED]

2026-06-10  FINAL-QA cross-instance pass. env: tpcds (region us-ashburn-1), DataLake …oc1.iad.<DATALAKE_OCID_REDACTED>…,
            workspace <WORKSPACE_UUID_REDACTED>; auth: oci raw-request --profile DEFAULT (api_key).
            NOTE: tpcds is a DIFFERENT instance than the 2026-06-09 <TENANCY> env — Preview-bucket (git/bundle/mlops)
            provisioning differences across instances are EXPECTED, not regressions. SQL ran on aidp_skilltest
            (DataLake …<DATALAKE_OCID_REDACTED>…, ws <WORKSPACE_ID_REDACTED>, cluster <CLUSTER_ID_REDACTED>, profile DEFAULT).
  -- rest-lake-governance (8/8 ok) --
  GET …/shares                          → 200 items=0   [data-sharing ✅]
  GET …/recipients                      → 200 items=0   [data-sharing ✅]
  GET …/roles                           → 200 incl. AUDITOR   [roles-access ✅; matches MCP list_roles]
  GET …/models?modelType=GENERATIVE_AI  → 200 items=0   [models-catalog ✅]
  GET …/credentials                     → 400 CannotParseRequest (route exists)   [credentials ⚠️ Preview]
  GET …/userSettings                    → 200 items=0   [user-settings ✅]
  GET …/agentFlowGuardrails             → 200 policy items (BLOCK hate/sexual/violence/toxic)   [guardrails ✅]
  GET …/knowledgeBases                  → 400 InvalidParameter (needs schemaKey & catalogKey)   [knowledge-bases ⚠️]
  -- rest-workspace-catalog (5 live / 3 Preview-404) --
  GET …/workspaces/<WS>/agentFlows      → 200 items=0   [agent-flows ✅ ws-scoped]
  GET …/workspaces/<WS>/tools           → 200 items=0   [tools ✅ ws-scoped]
  GET …/workspaces/<WS>/gitRepositories → 404 NotAuthorizedOrNotFound   [git ❌ Preview not provisioned on tpcds — expected]
  GET …/workspaces/<WS>/bundles         → 404 NotAuthorizedOrNotFound   [bundle ❌ Preview not provisioned — expected]
  GET …/workspaces/<WS>/mlops/api/2.0/mlflow/experiments/search → 404   [mlops ❌ Preview not provisioned — expected]
  GET …/catalogs                        → 200 incl. EXTERNAL catalog vector_db_*
  GET …/schemas?catalogKey=default      → 200 incl. schema acl_mini_dream
  GET …/tables?…&schemaKey=default      → 400 InvalidParameter: schemaKey must be <catalog>.<schema>
  GET …/tables?…&schemaKey=default.default → 200 items (e.g. default.default.web_site)   [FIX: fully-qualified schemaKey]
  -- MCP mirror (aidp server, tpcds-pinned) --
  list_catalogs 200 (4) ; list_schemas('default') 200 (102) ; list_tables(default,default.default) 200 (27 TPC-DS)
  list_workspaces 200 (4 ACTIVE) ; list_files('')/('Shared') 200 (root folder is 'Shared', not 'Workspace/Shared')
  list_clusters 200 (3: Default Master Catalog Compute ACTIVE/DEFAULT, tpcds ACTIVE, test STOPPED)
  get_default_cluster 200 ; list_jobs 200 (5 defs) ; list_recent_activities 200 (5)
  list_roles 200 (AI_DATA_PLATFORM_ADMIN, AUDITOR) — cross-checked vs REST /roles, identical (MCP/REST parity ✅)
  list_agent_flows 200 'No agent flows found' (valid empty, consistent with ws-scoped agentFlows items=0)
  list_volumes 400 Bad request on …/volumes (deterministic, 2× retry)   [volumes ⚠️ route reachable, rejects bare list]
  -- sql-exec (aidp_skilltest, scripts/aidp_sql.py, profile DEFAULT) --
  spark.sql SELECT COUNT(*) FROM default.default.deal_procurement_lifecycle_fact → ok, value 50, job 506   [analyzing-data ✅]
  spark.sql SELECT ai_generate('openai.gpt-5.4','reply with the single word OK') → ok, text 'OK', job 507   [ai-sql ✅]
  -> Net: GA categories + models-catalog + catalog/schema/table CRUD + Spark SELECT + ai_generate all LIVE on a
     2nd instance. Only Preview buckets (git/bundle/mlops) 404 = not provisioned there (expected, not a regression).

2026-06-10  DE-AGENT cross-instance pass. env: a FRESH instance PROVISIONED VIA THE PLUGIN (oci ai-data-platform
            create) in compartment <COMPARTMENT> (region us-ashburn-1). DataLake
            …oc1.iad.<DATALAKE_OCID_REDACTED>, ws <WORKSPACE_UUID_REDACTED>,
            cluster de_agent_cluster (USER, Spark 3.5.0, ACTIVE); auth: oci raw-request --profile DEFAULT (api_key).
            Confirms the plugin works on an instance it created itself, not just pre-existing <TENANCY>/tpcds.
  -- control-plane (oci raw-request) --
  GET …/shares · /recipients · /roles · /models?modelType=GENERATIVE_AI · /userSettings · /agentFlowGuardrails → 200
  GET …/catalogs · /schemas · /workspaces · /clusters (list+detail+libraries) → 200
  GET …/workspaces/<WS>/agentFlows · /workspaces/<WS>/tools → 200
  POST …/workspaces/<WS>/tools (create) → 200  +  DELETE …/workspaces/<WS>/tools/<key> → 204   [tools round-trip ✅]
  GET …/tables?catalogKey=default&schemaKey=default.default → 200 (de_customers)   [fully-qualified schemaKey confirmed]
  GET …/credentials → 400 (route exists) ; …/knowledgeBases → 400 (needs schemaKey+catalogKey) ; …/volumes → 400 (needs catalogKey+schemaKey)
  GET …/{git,bundle,mlops} → 404  (Preview not provisioned on fresh instance — expected)
  GET …/asyncOperations?status=|?resourceType= → 200 (observability) ; …/recentActivities → 404
  -- pipelines full lifecycle (jobs) --
  POST …/jobs → 201  →  PUT …/jobs/<k> (FULL replace, MUST include name+path) → 202  →  POST …/jobRuns {jobKey} → 201
     →  poll …/jobRuns/<id> SUCCESS  →  GET+POST …/taskRuns/<k>[/actions/fetchOutput] → 200  →  DELETE …/jobs/<k> → 204
  NOTE: documented …/jobs/<k>/actions/run → 404 here; use POST …/jobRuns {jobKey} instead.
  -- ingest (correct endpoint names) --
  POST …/actions/generate-temp-file-upload-target → 200  →  PUT PAR → 200  →  POST …/actions/infer-with-preview → 200
     (uses location=ociFilePath, NOT uploadKey)  →  POST …/actions/create-data-table → 202→SUCCEEDED
  Headerless CSV: tableFields come back _c0/_c1/_c2 then ALTER … RENAME. Old names uploadDataFile/inferSchema/createTable are WRONG.
  -- workspace-files (raw HTTP /notebook/api/contents) --
  bare HTTP …/notebook/api/contents CRUD → 500 (list) / 404 (put/get/delete) for api_key raw-request — use WebSocket helper / PAR.
  -- spark-debugging (kernel-side Spark UI REST: uiWebUrl + /api/v1) --
  /applications · /jobs · /allexecutors · /environment → 200 (1 app, 120 jobs, 2 executors)
  -- data-plane (scripts/aidp_sql.py on de_agent_cluster) --
  SHOW TABLES · GROUP BY · profiling (min/max/avg/null%) · data-quality (not-null+uniqueness) · DESCRIBE ·
     CREATE VIEW · JOIN (federate) · ai_generate('openai.gpt-5.4') → all ok with real results
  NOTE: brand-new clusters intermittently return "Command execution failed on compute cluster" on a busy default
     scratch notebook — use a UNIQUE --notebook path per call.
  -- agent-flows write surface (NEW create/deploy contract, was un-round-trippable on prior instances) --
  POST …/workspaces/<WS>/agentFlows {displayName,pathInfo} → 409 IncorrectState AiFeatureStatus=None on de-agent
     (datalake's Enable-AI-Feature workflow not yet complete — platform-provisioning state, "try again later", NOT a body defect).
     En route, the body contract was verified: create requires top-level {displayName, pathInfo} (description optional);
     pathInfo is a REQUIRED non-empty path segment (NOT a nodes array; "/" rejected → 400); both displayName & pathInfo
     validated against ^[A-Za-z][A-Za-z0-9_.-]*$ (same rule as tools API).
  POST …/actions/deployAgentFlow → 400 "deploymentType must not be null" (agentFlowKey alone insufficient);
     deploymentType is REQUIRED — valid enum is NOT AI_COMPUTE/SERVERLESS/DEDICATED/ON_DEMAND/QUICK_START/STANDARD/DEFAULT
     (all 400 "Invalid DeploymentType"); pull the real enum from SDK DeploymentType/Deployment models (undocumented in plugin refs).
  -> Net: ~22 skills PASS live on a plugin-provisioned instance; 4 route-exists-needs-params (credentials/knowledgeBases/volumes/+);
     3 Preview-404 expected (git/bundle/mlops); 0 unexpected failures. Whole agentFlows WRITE surface (create included) is gated
     on the DataLake aiFeatureStatus=Ready — a 200 on GET …/agentFlows does NOT imply create will succeed.
```

```
2026-06-12  PR#1 (v0.4.4 tester-feedback) CLAIM RE-VERIFICATION — independent THIRD instance, maintainer pass.
            env: tpcds (region us-ashburn-1), DataLake …oc1.iad.<DATALAKE_OCID_REDACTED>…, workspace <WORKSPACE_ID_REDACTED>,
            cluster `tpcds` key <CLUSTER_ID_REDACTED> (ACTIVE); oci raw-request --profile DEFAULT (api_key) + scripts/aidp_sql.py.
  -- DOC-2: default guardrails (lake-scoped) --
  GET …/20240831/dataLakes/<OCID>/agentFlowGuardrails → 200, EXACTLY 5 items (CONFIRMS the v0.4.4 aidp-agent-flows table):
     CONTENT_MODERATION        | USER_REQUEST   | BLOCK   (Content Moderation prevention)
     CONTENT_MODERATION        | AGENT_RESPONSE | BLOCK   (Content Moderation prevention)
     PROMPT_ATTACKS_PREVENTION | USER_REQUEST   | BLOCK   (Prompt Injection prevention)
     PII_DETECTION             | AGENT_RESPONSE | INFORM  (PII detection)
     PII_DETECTION             | USER_REQUEST   | INFORM  (PII detection)
     (a logical scope:BOTH PII policy materializes as two scope-specific rows → 5 total, not 3.)
  -- DOC-1: ai_generate independent of the /models catalog --
  GET …/models?modelType=GENERATIVE_AI → 200 items=0 (empty) AND
  spark.sql("SELECT ai_generate('openai.gpt-5.4','…')") → ok, text "hello from aidp", spark_job_ids=[0]
     → CONFIRMS ai_generate runs while the REST /models catalog is empty (resolves at the Spark engine).
  -- BUG-2: SHOW TABLES qualification (the Spark-SQL command, NOT the REST /tables endpoint) --
  spark.sql('SHOW TABLES IN default')         → AnalysisException [SCHEMA_NOT_FOUND] "The schema  cannot be found…"
  spark.sql('SHOW TABLES IN default.default') → ok (returns the TPC-DS table list)
     → CONFIRMS the v0.4.4 aidp-analyzing-data caveat for the SHOW TABLES SQL command.
  -- helper quirk (separate follow-up, as PR#1 itself flagged) --
  scripts/aidp_sql.py reports a cell that throws AnalysisException as {"status":"ok","error":null} with the
     traceback only in the stderr stream — reproduced here on the `SHOW TABLES IN default` cell. Not yet fixed.
  -- UX-2: cluster displayName charset (NOW VERIFIED — was tester-reported) --
  POST …/workspaces/<WS>/clusters {displayName:"etl-cluster", …} → 400 InvalidParameter, EXACT message:
        "Invalid resource name. Must start with letter and no special characters are allowed except for underscore, slash."
     → CONFIRMS the v0.4.4 aidp-cluster-ops claim IN FULL (start-with-letter + underscore/slash; hyphen rejected; 400 is
       synchronous, no cluster created). The cluster validator is STRICTER than the tools/agentFlows regex
       ^[A-Za-z][A-Za-z0-9_.-]*$ (which allows hyphens/dots) — a distinct surface, NOT a contradiction.

2026-06-12  BUG-1 FRESH-INSTANCE REPRO ATTEMPT — provisioned a brand-new DataLake to test the first-DDL claim.
            env: NEW DataLake `bug1_fresh_repro` (…oc1.iad.<DATALAKE_OCID_REDACTED>…, created+ACTIVE 2026-06-12),
            default workspace `bug1_ws` (<WORKSPACE_ID_REDACTED>), fresh USER cluster `bug1_cluster` (<CLUSTER_ID_REDACTED>, ACTIVE);
            oci ai-data-platform CLI to provision; scripts/aidp_sql.py for the DDL.
  -- BUG-1: first bare CREATE TABLE on a genuinely fresh instance --
  spark.sql('CREATE TABLE default.default.bug1_probe (id INT, name STRING) USING DELTA')  → status ok, spark_job 0,
        NO ArrayIndexOutOfBoundsException; `SHOW TABLES IN default.default` then lists `default.bug1_probe`.
     → COULD NOT REPRODUCE. The tester's ArrayIndexOutOfBoundsException did NOT occur even as the VERY FIRST DDL on a
       just-provisioned DataLake. Conclusion: transient/narrow post-provision race at most, NOT deterministic
       fresh-instance behavior — the aidp-sql-ddl caveat was softened accordingly (reported-once, not-reproduced).
  NOTE: instance `bug1_fresh_repro` left running (per standing "do not delete/stop" + "cost not a concern"); tear down on request.
```

```
2026-06-12  PR#2 (v0.5.0) MAINTAINER RE-VERIFICATION — env: tpcds (us-ashburn-1), DataLake …<DATALAKE_OCID_REDACTED>…,
            workspace <WORKSPACE_ID_REDACTED>, cluster `tpcds` <CLUSTER_ID_REDACTED> (ACTIVE); oci raw-request --profile DEFAULT (api_key) + scripts/aidp_sql.py.
  -- catalog-extractor correction (the PR's headline doc fix) --
  GET …/20240831/dataLakes/<OCID>/extractors          → 200 {"items":[]}              (CONFIRMS the correct path)
  GET …/20240831/dataLakes/<OCID>/metadataExtractors  → 404 NotAuthorizedOrNotFound   (CONFIRMS the old note probed the WRONG path)
  -- Spark-UI gateway proxy (control-plane alternative to kernel-side uiWebUrl) --
  GET https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/sparkui/<CLUSTER_ID_REDACTED>/api/v1/applications
      → 200, running app (appSparkVersion 3.5.0, completed:false, sparkUser dataflow)  (CONFIRMS the gateway proxy)
  -- session-token auth code (scripts/aidp_sql.py) --
  api_key DEFAULT:  spark.sql('SELECT 1') → status ok, real spark_job  (REGRESSION PASS — api_key path byte-for-byte unchanged).
  session AIDP_SESSION:  helper takes the session branch (builds a SecurityTokenSigner, NO KeyError); create_session 401'd only
      because the local session token was EXPIRED + non-refreshable headlessly — construction is correct, expiry is the only failure.
      End-to-end session-token success was verified by the PR author on their instance (OASECEAL); not re-verifiable in this env.
  jobs:  GET …/workspaces/<WS>/jobs → 200 (tpcds holds 5 jobs; the "100+/page pagination" lesson is playground-specific but sound).
```

**Net:** GA categories (shares/recipients/roles) + models-catalog are live-verified on `20240831/dataLakes`.
credentials route exists (shape TBD). git/bundle/agent-flows REST are not provisioned in this `20240831`
tenancy (agent-flows read still works via MCP). Re-probe credentials/mlops shapes when those features are
needed, and re-test `20260430` once the tenancy is upgraded to GA.
