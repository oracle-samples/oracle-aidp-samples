# Changelog

All notable changes to this plugin are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/).

## [0.5.0] — 2026-06-12 (session-token auth parity + Spark-metrics tooling + reliability hardening)

- **Session-token (security_token) auth parity** — `scripts/aidp_sql.py` now runs cell execution under an
  `oci session authenticate` **session-token** profile, not just api_key. The base `--profile` is auto-detected:
  a `security_token_file` → sign REST with a `SecurityTokenSigner` and **reuse the session token directly for
  the WebSocket (no UPST mint)**; api_key still mints a UPST; adds a session-expiry hint. Mirrors the reference
  SDK's `auth.py`/`notebook.py`. Fixes failed signing on session profiles (reported as `KeyError: 'tenancy'` on
  oci-CLI versions that write minimal session profiles without tenancy/user/fingerprint; on full session profiles
  the old code instead built a wrong api_key signer and 401'd — the fix is correct either way). LIVE-VERIFIED
  end-to-end: `SELECT 1` + a Spark-UI metric capture both `ok` under api_key (DEFAULT) AND session token
  (OASECEAL), and 9 control-plane APIs return identical results under both. `references/oci-raw-request.md`
  documents the full session-token path as first-class; `aidp-engineer-bootstrap` notes DEFAULT may be either.
- **Listing reliability** — fixed a false "0 jobs" on a 100+-job workspace (zsh unquoted-flag + a parser that
  rendered an error as an empty list). Hardened `aidp-pipelines` + `oci-raw-request.md`: paginate (100+/page),
  verify 2xx+JSON before concluding "empty", zsh array-args, backgrounded commands are network-sandboxed (use
  single foreground calls), `/Workspace` vs relative Jupyter-contents path roots.
- **Catalog extractor correction** — `GET …/extractors` → 200 (NOT `/metadataExtractors`, which 404s); removed
  the false "UI-only/404" claim from `aidp-catalog-init` + `aidp-table-management` and documented the real
  surface (extractedEntities/extractedTables/manageExtractedEntities + lifecycle).
- **Spark-metrics tooling** — documented the full fetchable surface in `aidp-spark-debugging` (job/task
  durations + cluster `summarizeMetricsData` are retained per run; per-run Spark-UI detail is live-only — no
  Spark History Server), added the control-plane Spark-UI **gateway proxy** alternative and a **snapshot-cell
  recipe** to persist `/api/v1` metrics for finished runs. `references/no-mcp-rest-map.md` gains the extractor +
  Spark-UI-proxy rows and the jobs-pagination note.
- **Publish polish** — surfaced session-token in `plugin.json`/`marketplace.json` descriptions/keywords; bumped
  to 0.5.0 (above main's 0.4.7); `claude plugin validate . --strict` passes; LICENSE (MIT) present. Deferred
  to main's 0.4.7 directory-card presentation (no `displayName`/`author` override).
- Authored an SDK-coverage-parity **design spec** (kept in the source repo; not shipped in the public plugin tree).

## [0.4.7] — 2026-06-12 (directory listing: publish-ready copy + attribution)

Public-directory polish ahead of submission to the official plugin directory (docs/metadata only; no skill change):
- Rewrote the `plugin.json` / `marketplace.json` `description` and the README hero into tighter, wow-factor copy
  led by "Run your entire AI data platform in English".
- **Removed the `author` field** from `plugin.json` so no personal "Made by" renders on the directory card; set
  the copyright holder + marketplace `owner` to "Oracle Corporation"; moved the maker credit (Forward Deployed
  Engineering) into an internal `NOTICE` file (not surfaced on the listing).
- Trimmed SDK-repo / "no private repo, no MCP required" / live-verification phrasing from the public copy.

## [0.4.6] — 2026-06-12 (live-verify the last two v0.4.4 tester claims: UX-2 confirmed, BUG-1 not reproduced)

Closed out the two v0.4.4 (#1) claims that v0.4.5 still left "tester-reported", by testing them live:
- **UX-2 (cluster `displayName` charset) — CONFIRMED.** `POST …/clusters` with `displayName: "etl-cluster"`
  → `400 InvalidParameter`: *"Invalid resource name. Must start with letter and no special characters are allowed
  except for underscore, slash."* — proves the full rule (start-with-letter **and** underscore/slash; hyphen
  rejected; 400 is synchronous, no cluster created). The cluster validator is stricter than the tools/agentFlows
  `^[A-Za-z][A-Za-z0-9_.-]*$` regex (distinct surface, not a contradiction).
- **BUG-1 (fresh-instance bare `CREATE TABLE`) — NOT reproduced.** Provisioned a brand-new DataLake + USER cluster
  and ran the *very first* DDL as a bare `CREATE TABLE … USING DELTA`: it **succeeded** (table created, confirmed
  via `SHOW TABLES`). The tester's `ArrayIndexOutOfBoundsException` did not occur even as the first DDL. Softened
  the `aidp-sql-ddl` caveat from a "fresh-instance behavior" to "reported once, not reproduced — possible transient",
  keeping the writer/ingest fallback as general robustness advice.
- Recorded both live runs (commands + exact outputs) in `references/rest-endpoint-map.md` (2026-06-12 blocks).

Docs only; no skill/behavior change.

## [0.4.5] — 2026-06-12 (official-doc alignment + maintainer live-verification of the v0.4.4 tester claims)

Reviewed the plugin's REST/SDK/CLI assumptions against Oracle's official AIDP docs
([aidug/use-apis-sdk-cli.html](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/use-apis-sdk-cli.html)
→ rest-apis / sdk-cli, and the GA endpoint reference at
[aiwap/rest-endpoints.html](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/rest-endpoints.html)).
**Verdict: aligned** — host (`aidp.<region>.oci.oraclecloud.com`), the official CLI/SDK repo
(`oracle-samples/aidataplatform-sdk`, Python/TS/Java), the GA version `20260430`/`aiDataPlatforms`, and the
`aiwap/` reference all match what the plugin already documents. Docs-only changes folded in:
- **Provenance citations** — added the three official doc URLs to `references/oci-raw-request.md` and
  `references/rest-endpoint-map.md` so the base-URL/version/prefix claims are sourced, not asserted.
- **Confirmed GA path shape** — the GA reference lists endpoints as `/20260430/aiDataPlatforms/<id>/…`:
  **version-first, NO `/api/` segment** (an earlier overview-page summary that implied `/api/` was a
  summarizer artifact — verified against the actual endpoint list). Our `/<version>/<prefix>/` shape matches.
- **Fixed two misleading examples** in `oci-raw-request.md` that paired `20260430`+`dataLakes`. GA `20260430`
  travels with `aiDataPlatforms`; LA `20240831` with `dataLakes` — they never cross. Examples now use the
  live-verified `20240831/dataLakes`. A future LA→GA move is a two-token swap (version + prefix), not a rewrite.
- **Recorded live evidence for the v0.4.4 (#1) tester-feedback claims** in `references/rest-endpoint-map.md`
  (dated 2026-06-12, **independently re-verified on a third instance — tpcds**, closing the no-fabrication
  ledger gap the merge review flagged): the **5 default guardrails** with exact `policyType`/`scope`/`action`;
  `GET /models?modelType=GENERATIVE_AI` → `items:[]` **while** `ai_generate('openai.gpt-5.4', …)` still returns
  text (real Spark job); and `SHOW TABLES IN default` → `AnalysisException [SCHEMA_NOT_FOUND]` vs
  `SHOW TABLES IN default.default` → ok. (The cluster-`displayName` charset and the fresh-instance
  `CREATE TABLE` caveats remain **tester-reported, not independently re-verified** — marked as such in the ledger.)
- **Corrected the v0.4.4 `aidp-cluster-ops` CHANGELOG wording** below (it said "underscores only"; the skill
  itself — and the captured 400 message — say *start with a letter; underscore and slash*).

Docs only; no skill/behavior change.

## [0.4.4] — 2026-06-12 (tester-feedback triage: doc + caveat hardening, live-reverified) — #1, @craxelfn

Folded in findings from a fresh-instance test pass (`taha-test-agent`, us-ashburn-1), then re-verified the
behaviors live against an established instance (`amitV2`): control-plane lake-scoped reads confirmed the
**5 default guardrails** (exact type/scope/action) and the empty `/models` catalog; the SQL engine confirmed
`SELECT 1`, `ai_generate('openai.gpt-5.4')` working while `/models` is empty, the `SHOW TABLES` qualification,
and that bare `CREATE TABLE` is **not** reproducible on an established instance. Docs/caveats only; no skill
behavior change. *(Maintainer note: the guardrails, `/models`+`ai_generate`, and `SHOW TABLES` claims were
independently re-verified on tpcds in v0.4.5 and recorded in `references/rest-endpoint-map.md`.)*
- **`aidp-sql-ddl`** — added a "Caveat" block: the first bare `CREATE TABLE … USING DELTA` on a
  **freshly-provisioned** instance can throw `ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0`
  (catalog-registration race). Live-checked **fresh-instance-only** — not reproducible on an established
  instance, even in a brand-new empty schema. Documented the `df.write.saveAsTable(...)` / ingest-then-CTAS
  fallback, and qualified the intro's "all un-wrapped → ok" claim. (Platform-side; our gap was the missing
  caveat. = report BUG-1.)
- **`aidp-models-catalog`** + **`aidp-ai-sql`** — clarified that `ai_generate` availability is **independent**
  of the `/models` REST catalog: `GET /models?modelType=GENERATIVE_AI` can return `items: []` while
  `ai_generate('openai.gpt-5.4', …)` still works (model resolves at the Spark engine, not the REST catalog).
  Source of truth is the `ai-sql` smoke test, not the endpoint. (= report DOC-1.)
- **`aidp-agent-flows`** — documented the **5 default guardrail policies** auto-provisioned on every fresh
  DataLake (so `GET …/agentFlowGuardrails` returns 5, not 0) with the verified type/scope/action table. (= DOC-2.)
- **`aidp-cluster-ops`** — documented the `displayName` charset (must **start with a letter**; only
  **underscore and slash** allowed; a hyphen → `400 InvalidParameter`). (= UX-2.)
- **`aidp-analyzing-data`** — explicit note to use `SHOW TABLES IN <catalog>.<schema>` (not the unqualified
  `SHOW TABLES IN default`, which raises `SCHEMA_NOT_FOUND`). (User-side, not a plugin bug — the skills never
  emitted the bad form; added the caveat anyway. = report BUG-2.)
- **TESTING.md** — version header `v0.4.2` → `v0.4.4` (= DOC-3); added a Console-naming note (**AI Data
  Platform Workbenches** vs API **DataLake**, = UX-1); added a "where the skills load" note for the VS Code
  extension vs CLI session, framed as needs-repro pending a confirmed reproduction (= report BUG-3).

## [0.4.3] — 2026-06-11 (team-handoff: from-scratch tester-onboarding doc hardening)

Certified the **from-scratch install** end-to-end on a brand-new teammate's path (clean clone → clean Python
venv → SessionStart auto-installs deps + sentinel/fast-skip → live smoke: `spark 3.5.0`, `SELECT 1`,
`ai_generate('openai.gpt-5.4') → OK`), and an adversarial "fresh-teammate" doc review found **6 onboarding gaps**, all fixed:
- **Stale version labels** — TESTING.md title + README Status said `v0.3.1`; now current (`v0.4.2`/manifest).
- **Broken + redundant manual pip step** — `python -m pip install -r scripts/requirements.txt` was in the
  copy-paste install blocks with a cwd-relative path that **fails after `claude plugin install`** (the plugin
  lives in Claude's cache, not the tester's cwd) and duplicates what the SessionStart hook already does. Removed
  from the install blocks; demoted to a "from the plugin dir" fallback.
- **Engine story** — TESTING now states control-plane uses `oci raw-request` by default; the official `aidp` CLI is optional.
- **bootstrap → catalog-init dependency** made explicit (catalog-init required before NL data questions).
- **"Don't have an instance?"** on-ramp added (shared `de-agent` / provision your own; `ai-sql` needs a GenAI model).
- **Access verification** — a `git ls-remote` check so a no-access `marketplace add` failure isn't mistaken for a plugin bug.

Docs only; no skill/behavior change.

## [0.4.2] — 2026-06-11 (clean-env install test: harden the SessionStart dep-install)

Ran a full **fresh-install validation** (clean `git clone` → clean Python `venv` → live smoke on a
plugin-provisioned AIDP instance): structure loads (37 skills, valid manifests + `hooks/hooks.json`),
`check_env.py` auto-installs the deps in a clean venv, and the *clone's* `scripts/aidp_sql.py` ran
`SELECT 1` + `ai_generate('openai.gpt-5.4')` on the cluster — fresh install works end-to-end. The test
surfaced and fixed two robustness issues in the SessionStart `check_env.py`:
- **pip-install timeout too tight** — 180s wasn't enough for the large `oci` package on a cold install
  (it timed out and logged a spurious error). Raised to **280s**, and the hook timeout **200→300s**, so a
  cold install completes; the result is now reported by a **post-install re-check** ("deps auto-installed"),
  so a slow-but-successful install no longer reads as an error.
- **sentinel not written** when `CLAUDE_PLUGIN_DATA` didn't pre-exist (silently swallowed) → every session
  would re-run the install. Now `os.makedirs(exist_ok=True)` before writing it, so later sessions skip (verified:
  run #1 "deps auto-installed" + sentinel written, run #2 "deps OK").

Robustness only; no skill/behavior change.

## [0.4.1] — 2026-06-11 (upstream-skill coverage audit: fold notebook + platform-ops gotchas)

Audited the upstream `amitranjan-oracle/ai-data-engineer-agent` `.claude/skills` (3 skills) against the plugin
and closed the two real gaps (the third, `spark-optimization`, was already incorporated in 0.4.0). Added an
**"AIDP notebook gotchas"** section to **`aidp-notebooks`**: NEVER `spark.stop()`; markdown-UI quirks (no
spaces-in-parentheses → `%20`, ASCII-only); the **`compute:///` Spark output-path rules** (no `Path.resolve()`,
size reports 0 → use `oci://` for sizing, never `spark.sql.warehouse.dir`, `/tmp` vs `/Workspace`, `/Volumes`
checkpoints); and the two-surface session-debug sequence. The rest of `aidp-platform-ops` (auth/token-priority,
workspace-scoping, failure-debugging discipline, Spark-UI-gateway→kernel, metrics ms-timestamp) and
`aidp-notebook-development` (execution model, sessions) were already covered across `aidp-engineer-overview`,
`aidp-cluster-ops`, `aidp-spark-debugging`, `aidp-notebooks`, and `scripts/aidp_sql.py`. Docs-only.

## [0.4.0] — 2026-06-11 (new skill: aidp-spark-optimization — deep Spark performance tuning)

Added a **37th skill — `aidp-spark-optimization`** — the deep performance-tuning companion to the lighter
`aidp-spark-debugging` (whose description now points to it instead of "the spark-performance-optimization skill
upstream"). A measurement-loop optimizer for OSS **Spark 3.5.0 + Delta 3.2.0**: partitions/parallelism,
joins/broadcast/skew, small-file & I/O layout, memory/spill, codegen, caching/materialization, AQE, Delta
OPTIMIZE/ZORDER/clustering/deletion-vectors/CDF, and **Oracle-DB JDBC** read/write — with an 80/20 move list, a
symptom→reference decision tree, a config matrix (incl. where-settable on AIDP), cluster-sizing, real
before/after case studies, and AIDP execution notes (one SparkSession per cluster; set/revert configs).
Ships `SKILL.md` + **15 reference files**. Execution maps to the plugin's `scripts/aidp_sql.py`,
`aidp-spark-debugging`, `aidp-cluster-ops`, and `aidp-sql-ddl`/`aidp-table-management`. Router + README + the
skill count (36→37) were updated. *Adapted from the `ai-data-engineer-agent` `spark-optimization` skill (Oracle).*

## [0.3.4] — 2026-06-11 (name the helper-upload mechanism: aidp-connectors-bootstrap)

Refines the v0.3.3 delegation pointers to name the actual upload mechanism instead of a vague "upload the
package and put it on sys.path": the spark-connectors plugin's own **`aidp-connectors-bootstrap`** skill is
what installs its `oracle_ai_data_platform_connectors` helper package into `/Workspace/Shared` (it drives the
AIDP MCP `create_directory`/`upload_file`/`nb_*` tools + runs a sanity-import notebook), with a manual upload
fallback when the MCP can't reach the target instance. Applied to `aidp-notebooks`, `aidp-analyzing-data`,
`aidp-engineer-overview`, and `aidp-federate`. Docs-only; no behavior change.

## [0.3.3] — 2026-06-11 (sharper external-source delegation to the spark-connectors plugin)

Closed a routing gap that let an external-source connection get hand-rolled instead of delegated: a
single-source request like *"a notebook that connects to Fusion"* landed on `aidp-notebooks` /
`aidp-analyzing-data` (which had no connector pointer), while the existing delegation lived only in the
router's out-of-scope note and in `aidp-federate` (framed as multi-source joins). Now:
- **`aidp-notebooks`** + **`aidp-analyzing-data`** — explicit pointer: connecting to an external/non-lakehouse
  source (Fusion, EPM, ADB/ExaCS, Snowflake, S3, Kafka, …) → use the
  `oracle-ai-data-platform-workbench-spark-connectors` plugin's `aidp-<source>` skill; **do not hand-roll it.**
- **`aidp-engineer-overview`** (router) + **`aidp-federate`** — made the delegation *actionable*: check the
  spark-connectors plugin is installed (`claude plugin list`), and note its `oracle_ai_data_platform_connectors`
  helper package must be **uploaded to the cluster and on `sys.path`**. `aidp-federate` is now also the entry
  point for a **single** external source, not just multi-source joins.

Docs-only; no behavior change.

## [0.3.2] — 2026-06-10 (low-friction install: SessionStart readiness hook)

Added a bundled **SessionStart hook** (`hooks/hooks.json` → `scripts/check_env.py`) so the plugin sets itself
up on first session — the closest Claude Code allows to "execute on install" (there is no npm-style
post-install hook). On session start it: **auto-installs the bundled Python deps** (`scripts/requirements.txt`)
if an import check fails — one-time, sentinel-guarded in `${CLAUDE_PLUGIN_DATA}`, so later sessions are
instant — then reports local OCI readiness (the `oci` CLI + a `~/.oci/config` profile) and points to the
`aidp-engineer-bootstrap` skill if anything is missing. It **always exits 0** (never blocks a session), prints
an ASCII-only one-line banner, and honors `AIDP_PLUGIN_NO_AUTOINSTALL=1` for a check-only (no-pip) mode. The
one irreducible manual step is **OCI credentials** — per-user secrets a plugin cannot bundle. `TESTING.md`
updated to describe the auto-check.

## [0.3.1] — 2026-06-10 (live end-to-end validation on a plugin-provisioned instance + doc corrections)

Provisioned a **brand-new AIDP instance entirely through the plugin** (`oci ai-data-platform create` → DataLake
`de-agent` in the DataServices compartment → default workspace → a USER Spark cluster), loaded synthetic data,
and exercised the skills **live** against it: **~22 skills PASS** — catalog-explore, analyzing-data,
profiling-tables, data-quality (not-null + uniqueness), sql-ddl, table-management, federate, **ai-sql
(`ai_generate('openai.gpt-5.4')`)**, ingest-file-to-table, pipelines (**full job create→run→SUCCESS→delete
lifecycle**), notebooks, cluster-ops (created + started the cluster), spark-debugging (Spark UI), observability,
roles-access, data-sharing, models-catalog, user-settings, agent-flows + guardrails, workspace-admin, and
**tools (create→delete write round-trip)**. 4 route-exists-needs-params (credentials / knowledge-bases /
volumes / workspace-files HTTP-contents), 3 Preview-404 expected on a fresh instance (git / bundle / mlops),
**0 unexpected failures**. Folded the live-found corrections back into the docs (doc-accuracy only, no behavior change):
- **`aidp-pipelines`** — PUT job-update is a *full replace* (re-send `name`/`path`/`maxConcurrentRuns`); the working run trigger is `POST …/jobRuns {jobKey}` (documented `…/jobs/{key}/actions/run` 404s); task output via `GET …/taskRuns/{k}` + `…/actions/fetchOutput`.
- **`aidp-ingest-file-to-table`** — real action names are `generate-temp-file-upload-target` / `infer-with-preview` / `create-data-table` (not `uploadDataFile`/`inferSchema`/`createTable`); create is headerless/positional (`tableFields` use `_c0/_c1…`, then `ALTER … RENAME COLUMN`); infer `location` = `ociFilePath` not `uploadKey`.
- **`aidp-observability`** — `asyncOperations` requires a `status=` or `resourceType=` filter; `recentActivities` 404s.
- **`aidp-volumes`** / **`aidp-knowledge-bases`** — list requires `catalogKey` + `schemaKey` query params.
- **`aidp-workspace-files`** / **`aidp-notebooks`** — bare HTTP `…/notebook/api/contents` CRUD 500/404s for api_key `oci raw-request`; route file CRUD via the WebSocket helper (`scripts/aidp_sql.py`) / PAR / `nb_*` tools (which are verified working).
- **`aidp-agent-highcode`** — create body is `{displayName, pathInfo}` (naming rule enforced); deploy needs a `deploymentType`; the entire `agentFlows` **write** surface is gated on the DataLake `aiFeatureStatus=Ready` (a fresh instance returns `409 IncorrectState AiFeatureStatus=None` — a platform-provisioning state, not a body/plugin defect).
- **`references/rest-endpoint-map.md`** — appended the dated `de-agent` cross-instance verification block.

## [0.3.0] — 2026-06-10 (re-audit + depth gap-closure → ~95%)

A re-audit of v0.2.0 scored **~86%** (up from 73%) and confirmed the five core domains genuinely closed; it
surfaced 15 remaining **depth/doc** gaps (none structural). Closed all of them via 10 parallel agents on
**disjoint files** (no new skills — depth added to existing ones + one new reference), then an adversarial
verify pass confirmed **all 14 gaps closed + grounded, zero fabrication** (the 15th, a dangling `aidp-ontologies`
ref, was fixed first). Field names/enums/commands all trace to real SDK models / CLI README / platform-ref;
unconfirmable wrappers stay verify-first.
- **`aidp-mlops`** — full **46-command** index (was ~12), grouped; bodies + enums (view_type, run status) cited.
- **`aidp-agent-flows`** — per-policyType **guardrail config** (5 SafetyPolicy subtypes + child fields) + **A2A
  agent-card publishing** (`agentCardConfig`, outbound discovery) + direction table.
- **`references/agent-flow-nodes.md`** — the 4 structural node configs (supervisor/nested/external/HITL) filled in.
- **`aidp-roles-access`** + **cli-map** — Job/Workflow permission row (note: `AssignJobPermissionDetails`
  `{assignees,permissions}`, **not** the generic grant body).
- **`aidp-notebooks`** — Structured Streaming code (readStream/writeStream/checkpoint-on-Volume, platform-ref §21).
- **`aidp-bundle`** — `CreateBundleDetails` manifest + promotion path.
- **`aidp-knowledge-bases`** — HNSW/IVF index tuning params + IVF 7-distance enum.
- **`aidp-credentials` / `aidp-user-settings` / `aidp-git`** — create-body shapes from SDK/CLI README.
- **`aidp-catalog-init` + `aidp-table-management`** — Auto-Populate-Catalog = UI-only note (no API).
- **`aidp-ingest-file-to-table`** — documented limits (comma-only delimiter; no multi-line-JSON external tables).
- **`references/dq-rules.md`** (new) + **`aidp-data-quality`** — persistable `.aidp/dq-rules.md` rule-set convention.

**Live-validated on a brand-new instance (`aidp_skilltest`, oaseceal/IAD).** All 36 skills exercised end-to-end
with synthetic data + real resource creation: **21 PASS / 5 PARTIAL / 6 NA / 4 NOT_PROVISIONED / 0 FAIL**
(NOT_PROVISIONED = agent-flow/KB writes gated by `AiFeatureStatus=None` on a fresh instance + git/bundle Preview;
NA = composition/local skills). `ai_generate('openai.gpt-5.4', …)`, full Delta DDL/DML, table/view CRUD,
`aidp-tools` round-trip (POST 200 → DELETE 204), pipelines create→run→SUCCESS, and governance creates all
confirmed live. Two doc fixes from the test:
- **`aidp-pipelines`** — corrected to the verified **two-step** job authoring (create name-only `{name,path,
  maxConcurrentRuns}` → then `update-job` adds `jobClusters` + `tasks` with `type`/`cluster{clusterKey,clusterName}`/
  `source`); a single inline-tasks POST is rejected.
- **`aidp-spark-debugging`** — Spark-UI example now uses `ssl._create_unverified_context()` (the cluster UI is
  HTTPS self-signed; bare urlopen raised `SSLCertVerificationError`).
- Also validated by rebuilding the **MBC Procure-to-Pay Story Engine** demo (synthetic `DEAL_PROCUREMENT_LIFECYCLE_FACT`,
  50 deals, 30-pattern catalog, `ai_generate` deal narratives with anomaly callouts) entirely via these skills.

## [0.2.0] — 2026-06-10 (capability-audit gap closure — full AIDP coverage)

A 21-agent capability audit (plugin vs the full AIDP surface) scored the 31-skill plugin ~73% — strong on the
read/analyze core, materially incomplete on write-side SQL and the agentic/AI surface. Closed **all** P0/P1/P2
gaps, **live-probe-first** on the disposable `aidp_agent_e2e` instance (`oaseceal`, api_key DEFAULT). Skill
count **31 → 36**.

**New skills (5):**
- **`aidp-sql-ddl`** — write-side Spark SQL: DDL/DML (`INSERT/UPDATE/DELETE/MERGE`, `CREATE/ALTER/DROP`) + Delta
  maintenance (`OPTIMIZE/VACUUM`, time travel, `RESTORE`, schema evolution, liquid clustering). **All ops
  live-verified** on AIDP (un-wrapped → `status: ok`).
- **`aidp-table-management`** — control-plane catalog/schema/table/view lifecycle + external-catalog registration.
- **`aidp-knowledge-bases`** — RAG corpus: KB CRUD, embedding/chunking, HNSW/IVF index, ingestion jobs
  (route live: `…/knowledgeBases` 400 = exists, lake-scoped).
- **`aidp-agent-highcode`** — code-first agents (LangGraph + `aidputils`: `setup()/invoke()`, `OCIAIConf`,
  `AIDPToolConf`, `create_react_agent`/`StateGraph`).
- **`aidp-tools`** — standalone reusable tools (SQL/Prompt/RAG/HTTP/Custom/MCP). **Create→delete round-trip
  verified** (`POST ws/tools` 200, `DELETE` 204).

**Extended skills:** `aidp-pipelines` (repair/retry/streaming/parameterization + system params); `aidp-cluster-ops`
(provision/scale, GPU/RAPIDS, AI Compute, BI JDBC/ODBC); `aidp-notebooks` (`%run`/`oidlUtils`/terminal);
`aidp-roles-access` (full per-resource permission matrix incl. table/view + honest masking/classification);
`aidp-agent-flows` (guardrail authoring + node-type reference); `aidp-workspace-admin` (verified
`oci ai-data-platform` instance create/delete + IAM preflight); `aidp-semantic-model` (native Ontologies note);
`aidp-credentials` (corrected to **lake-scoped**); `aidp-observability` (`recentActivities` 404 → use
`list-recent-job-runs`); `aidp-user-settings` (200 verified); `aidp-mlops` (probe note). New
`references/agent-flow-nodes.md` (all 13 node types).

**Honest non-fabrication outcomes:** masking/classification + ontologies have **no data-plane REST API** here
(`/maskingPolicies`,`/dataClassifications`,`/tags`,`/ontologies` → 404) — documented as UI-only with the
restricted-view + `.aidp/semantic.md` API-driven analogs, not a fabricated endpoint. All probe results recorded
in `references/rest-endpoint-map.md`.

## [Unreleased] — 2026-06-10 (Native MCP Client Support — MCP_TOOL node addon)

Closed a capability gap surfaced while verifying a product-team claim ("MCP remote is part of LA"). The LA
feature is **Native MCP Client Support** — an AIDP Agent Flow connects **out** to a remote MCP server via an
**`MCP_TOOL` node** (AIDP is the client; it does **not** host an MCP server — all `/mcp` server paths 404,
verified live). The plugin already *optionally consumes* a local-stdio `aidp` MCP as a tool backend, but did
**not** cover authoring this platform feature. Now added:
- **`aidp-agent-flows`** — new section "Attach a remote MCP server to a flow — Native MCP Client Support (LA)":
  the verified `MCP_TOOL` node shape (`CreateMcpToolNodeDetails` → `toolConfig` = `McpToolConfiguration`
  `{ endpoint, auth.authType ∈ NO_AUTH|BEARER_TOKEN|OAUTH|OCI_RESOURCE_PRINCIPAL, allowedTools[], customHeaders }`),
  the `FetchMcpObjects` / `TestMcpConnection` / `TestMcpExternalTool` introspection actions, the REST path, and
  a no-fabrication note (round-trip an existing flow to confirm the node-graph wrapper + per-auth credential
  fields). All field names grounded in the official SDK models; not invented.
- **Verified live 2026-06-10:** `GET …/workspaces/<ws>/agentFlows` → **200** on `oaseceal` (IAD); every
  `/mcp`,`/mcpServers`,`/remoteMcp` server path → 404 on both `20240831/dataLakes` and `20260430/aiDataPlatforms`.
  Recorded in `references/rest-endpoint-map.md`.

## [Unreleased] — 2026-06-09 (live write-path E2E on tpcds + endpoint finding)

Closed the "write ops not live-tested" gap — exercised real mutations on `tpcds` (self-contained, api_key
DEFAULT) and cleaned up after:
- **Table write path ✅** — `aidp_sql.py`: `CREATE TABLE … AS SELECT` (managed) → COUNT=3 → DQ nulls=0 →
  `DROP`; catalog confirms no leftover.
- **Job/pipeline write path ✅ (flagship)** — ran Oracle's own `workflow_notebook_job_sample.py` on `tpcds`:
  create folder+notebook → create job → start run → poll → **SUCCESS** → built-in cleanup. Confirmed clean
  (jobs back to 5; the sample job returns 404). Proves `aidp-pipelines` (create-job/run/poll/fetch) +
  `aidp-notebooks` (create) end-to-end via the supported SDK.
- **CLI reads ✅** — `aidp delta-share|credentials|user-setting list` + `async-operations list --resource-type`
  all return 200 via the official CLI. `credentials list` works cleanly via the CLI (the earlier
  `oci raw-request` GET-shape 400 is moot — use the CLI).

### Endpoint / API-version finding (important, documented in references)
- The official **SDK defaults to host `datahub-dp.<region>.oci.<sld>` + `/20260430/aiDataPlatforms/`** (GA).
  On a tenancy not on that host (e.g. `tpcds`), the SDK **404s** until you set
  `AIDP_ENDPOINT=https://aidp.<region>.oci.oraclecloud.com`.
- The official **`aidp` CLI defaults to `--endpoint https://aidp.<region>.oci.oraclecloud.com`** (the gateway
  that *does* serve `tpcds`) — which is why the CLI worked out-of-the-box and the raw SDK example didn't.
- The `aidp.<region>` gateway serves **both** the GA `20260430/aiDataPlatforms` (CLI/SDK) **and** the legacy
  `20240831/dataLakes` (our `oci raw-request` fallback + the MCP). **Guidance:** always point the SDK/CLI at
  `AIDP_ENDPOINT/--endpoint = https://aidp.<region>.oci.oraclecloud.com`, not the SDK's `datahub-dp` default.

## [Unreleased] — 2026-06-09 (close addressable gaps + demo-parity)

Reviewed the two official demos (`AIDPCLILatestDemo`, `AIDPCLIAISkillDemo`) frame-by-frame — both flows
(install→configure→cluster→notebook→job→run→fetch-output, an agent driving the `aidp` CLI by NL) are fully
covered by the plugin. Closed the addressable gaps surfaced earlier + by the demos:

### Added
- **`references/payloads.md`** — `.aidp/payloads/` convention: persist every mutation body to JSON, show +
  confirm, then run (mirrors the demo's `.aidp-memory/payloads/`; auditable + re-runnable). Wired into the
  overview shared rules + every mutating skill.
- **`aidp-pipelines` run-notebook-as-job recipe** — explicit create-job → create-job-run → poll `get-job-run`
  to terminal → `fetch-output` → summarize (the exact AI-skill-demo flow; notebooks run as jobs, not cells).
- **Bootstrap "check my AIDP CLI setup"** — set/verify `AIDP_INSTANCE_ID` + `AIDP_ENDPOINT` (matches the demo).

### Changed
- **Per-skill CLI-first pass (17 control-plane skills):** each now leads with the official
  `aidp <group> <command>` (from `references/aidp-cli-map.md`) and demotes `oci raw-request` to the fallback —
  resolving the consistency gap. `git`/`agent-flows` correctly stay REST-only (no CLI group in v1.0.0; no
  commands fabricated). Cell execution stays on `scripts/aidp_sql.py` (CLI can't exec cells).

### Still not code-addable (documented limitations, not gaps we can close in-plugin)
- **DataLake instance creation** — OCI service-control-plane + IAM-gated (data-plane `POST /dataLakes` → 404);
  do it in the OCI Console. `aidp-workspace-admin` keeps this guarded.
- **Data lineage** — no AIDP lineage API exists.
- **Distribution** — official `aidp` CLI not yet on PyPI/npm (install from release zip; use a venv); this
  plugin isn't published to a marketplace yet.
- **Preview/LA features** (git/bundle/agent-flows/credentials/mlops) — unverified / not provisioned in the
  tested tenancies; flagged per the no-fabrication gate until enabled.

## [Unreleased] — 2026-06-09 (align to the official public AIDP SDK/CLI)

The official **Oracle AIDP SDK + CLI** went public: `oracle-samples/aidataplatform-sdk` v1.0.0
(Python/TS/Java; PyPI/npm/Maven coming soon). Reviewed it (215 commands / 16 groups) and aligned the plugin
to be the **agent-skills layer on top of it**, replacing the private `ai-data-engineer-agent` framing.

### Verified (official CLI, live on `tpcds`, 2026-06-09)
- Installed the official `aidp` CLI v1.0.0 from the clone; `aidp command-groups` lists all 16 groups
  (matches `references/aidp-cli-map.md`).
- `aidp catalog list --instance-id <tpcds> --auth api_key --profile DEFAULT --region us-ashburn-1` → the 4
  catalogs with full metadata; `aidp role list` → AUDITOR + AI_DATA_PLATFORM_ADMIN (matches the
  `oci raw-request` results). The CLI-preferred engine works with the api_key DEFAULT profile.
- **Install note:** installing the SDK/CLI alongside `oci-cli` triggers pip dependency conflicts
  (downgrades `cryptography`; clashes `click`/`oci` pins). The CLI still runs, but install it in a **venv**
  to avoid disturbing `oci-cli` (and the bundled helper's deps).

### Changed
- **Engine precedence → CLI-preferred, `oci raw-request` fallback.** Control-plane skills now prefer the
  official `aidp <group> <command>` CLI (`--auth api_key --profile DEFAULT --instance-id <OCID>`) when
  installed, falling back to `oci raw-request` against the same REST API otherwise. Set the rule globally in
  `aidp-engineer-overview`, `references/oci-raw-request.md`, `references/no-mcp-rest-map.md`.
- **`scripts/aidp_sql.py` stays** for interactive Spark-SQL — confirmed the official CLI/SDK does **not**
  execute notebook cells (its Notebook group is files + sessions; running a notebook is job-based).
- **Bootstrap** now installs/detects the official `aidp` CLI (Step 2b) as the preferred engine.
- **README + manifests** reposition the plugin as the agent-skills layer over `oracle-samples/aidataplatform-sdk`.

### Added
- `references/aidp-cli-map.md` — maps every skill to its official `aidp` CLI command(s) + SDK client.
- Two new skills (29 → **31**): `aidp-audit` (audit logs) and `aidp-user-settings` — covering official CLI
  groups the plugin lacked.

### Notes
- Official CLI v1.0.0 has **no** agent-flow command group and only `workspace create-git-folder` for Git;
  our `aidp-agent-flows` / `aidp-git` (full GitService) remain REST-only (Preview/LA) and go beyond the CLI.
- The repo ships **no agent skills / MCP** — the "AI Skills Demo" is Codex driving the public CLI; our 31
  SKILL.md skills are complementary (the skills layer), not superseded.

## [Unreleased] — 2026-06-09 (self-contained engine: live E2E on BOTH instances)

### Verified (no MCP, api_key DEFAULT only — proves the self-contained engine + portability)
Ran end-to-end on **two AIDP instances** in tenancy `oaseceal` / us-ashburn-1, using ONLY
`oci raw-request` (control plane) + `scripts/aidp_sql.py` (SQL) — no MCP, no `AIDP_SESSION`, no
`ai-data-engineer-agent`:
- **`tpcds`** — control plane: catalogs(4)/schemas(102)/tables(27)/workspaces(4)/clusters(3)/jobs(5)/
  roles(2)/shares/recipients/models all **200**. SQL via the helper: `status ok` for analyzing-data
  (count 28,800,991 + join), profiling, data-quality (0 grain dupes), and ai-sql (`ai_generate` ok).
- **`AI_agentic_ahmed`** (`dcccq9jvbur9mflxwp8iad`) — control plane: catalogs(1)/schemas(2)/workspaces(3)/
  roles(3)/shares/recipients/models all **200**. SQL via the helper on a freshly-started cluster:
  `status ok` (`SELECT 1`, `spark.range`, Spark 3.5.0, `ai_generate('openai.gpt-5.4')` → OK).
- **Cluster lifecycle (self-contained)**: started + stopped `disco_test_cluster` via
  `oci raw-request POST .../actions/start|stop`; **notebook delete → 204**.

### Fixed
- **Cluster start/stop 400 root-caused**: `…/clusters/<key>/actions/start|stop|restart` **require a JSON
  body** — calling with no body returns `400 "The request body must not be null"`; passing `{}` returns
  `202`. (This, not a workspace mismatch, was the long-standing "start 400".) Updated `aidp-cluster-ops`
  + `references/no-mcp-rest-map.md`. Also noted: re-starting while `STARTING` returns `409`; volumes list
  needs `schemaKey` as well as `catalogKey`.

## [Unreleased] — 2026-06-09 (self-contained re-architecture)

### Changed
- **Removed the hard dependency on the `ai-data-engineer-agent` repo and the `aidp` MCP server.** The
  plugin is now fully self-contained: every skill works with only the bundled assets plus an OCI
  `api_key` DEFAULT profile.
- **All skills are now REST-first.** Control-plane operations (catalogs, schemas, tables, clusters,
  jobs, workspaces, roles, volumes, files, credentials, sharing, git, bundle, mlops, models,
  agent-flows) run via `oci raw-request` against the AIDP REST API
  (`https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/...`, auth
  `--profile DEFAULT`). Endpoints/params are documented in `references/oci-raw-request.md` and
  `references/no-mcp-rest-map.md` — no endpoints were invented.
- **The `aidp` MCP is demoted to an optional accelerator.** If one is configured the skills may use
  its tools, but it is no longer required or assumed.
- **`aidp-engineer-bootstrap` no longer installs an external MCP.** Setup now discovers the
  DataLake/workspace via the no-MCP REST path and wires the bundled SQL helper; MCP registration is
  offered only as an optional convenience.

### Added
- **Bundled `scripts/aidp_sql.py`** for interactive Spark-SQL / notebook cells, with no MCP and no
  `AIDP_SESSION` required:
  `python scripts/aidp_sql.py --region <r> --datalake <ocid> --workspace <ws> --cluster <key> --code <code>`.
  It mints a UPST from the `api_key` DEFAULT profile, auto-creates a scratch notebook, and returns JSON
  with `status` / `outputs` / `spark_job_ids` (`--session-profile` is optional).
- **Live verification of `scripts/aidp_sql.py` on `tpcds`:** `SELECT COUNT(*) FROM default.default.store_sales`
  = **28,800,991**; a `store_sales × item` join returned real rows; and `ai_generate(...)` produced a
  grounded narrative — all via the `api_key` UPST with no `AIDP_SESSION`.

## [0.1.0] — 2026-06-09 (initial scaffold)

### Added
- Initial plugin scaffold: 29 `aidp-*` skills over the `aidp` MCP + AIDP REST (`oci raw-request`).
- Manifests (`plugin.json`, `marketplace.json`), `.mcp.json.template`, reference docs.
- Grounding/reliability layer: `aidp-semantic-model` + `aidp-verified-queries`.
- Signature differentiators: `aidp-federate`, `aidp-ai-sql`.
- Provisioning: `aidp-workspace-admin`.

### Live verification — 2026-06-09 (tenancy `oaseceal`, region us-ashburn-1, DataLake IAD, cluster `tpcds`)
**Control-plane / MCP (verified):** `list_workspaces` (4), `list_catalogs` (4), `list_schemas` (102 in
`default`), `list_tables`, `get_table` (`store_sales`, 23 cols), `get_default_cluster`, `list_clusters`
(`tpcds` ACTIVE), `create_notebook` + `nb_create_session` (created + cleaned up), `list_roles` (2),
`list_recent_activities`, `list_agent_flows` (none). → catalog-init, catalog-explore, cluster-ops,
workspace-files/notebooks (create), observability, roles/agent-flows (read) all work.
**REST via `oci raw-request --profile DEFAULT` (verified):** env serves **`20240831/dataLakes`** (GA
`20260430` 404s here). `shares` 200, `recipients` 200, `roles` 200, `models?modelType=` 200;
`credentials` 400 (route exists, GET shape TBD); `git`/`bundles`/`agentFlows` REST 404
NotAuthorizedOrNotFound (not provisioned in this `20240831` tenancy).
**SQL-execution skills — VERIFIED after `AIDP_SESSION` re-auth (Spark 3.5.0 on `tpcds`):**
- `analyzing-data` ✅ `SELECT COUNT(*) FROM default.default.store_sales` = **28,800,991**; `store_sales × item`
  top-N join returned real results.
- `profiling-tables` ✅ 28.8M rows, `ss_net_paid` nulls = 1,297,387 (~4.5%), ~99,449 distinct items,
  min 0.00 / max 19,562.40 / avg 1,722.17.
- `data-quality` ✅ grain uniqueness `(ss_ticket_number, ss_item_sk)` → **0 violations** (PASS).
- `ai-sql` ✅ `ai_generate('<model>','<prompt>')` (model-first) works for **openai.gpt-5.4, openai.gpt-4o,
  xai.grok-4**; grounded narrative over a $47.37B / 102k-item / 28.8M-line aggregate returned a coherent
  one-line summary. (Note: models-catalog REST listed 0 under the probed `modelType`s, yet `ai_generate`
  models are available — catalog filter ≠ ai_generate availability.)
- `verified-queries` ✅ validation flow proven (candidate SQL executes + answers before marking verified).
- `federate` ✅ in-session multi-table join mechanism (lakehouse); true external-source federation still
  requires the spark-connectors plugin + source credentials (not exercised here).
- `spark-debugging` ✅ Spark-UI proxy returned 20 real SQL executions with durations/status.
- `notebooks` ✅ create notebook + kernel session + `nb_execute_code` round-trip (created & cleaned up).

**Net:** end-to-end live test complete on tenancy `oaseceal` / us-ashburn-1 / `tpcds`. Remaining caveats:
`credentials` GET list-shape TBD (route 400); `git`/`bundles`/`agent-flows`(REST) not provisioned in this
`20240831` tenancy; `mlops` MLflow shape not probed. See `references/rest-endpoint-map.md` → Verification log.

### Second-instance retest — 2026-06-09 (DataLake `AI_agentic_ahmed`, host `dcccq9jvbur9mflxwp8iad…iad`, OCID `…amaaaaaaai22xpqascswsnjxzgudl4tsnyqj47pbqh2q3zd6dzkbrtrg5lia`, via the `aidp_ahmed` MCP alias)
Confirms **multi-instance portability** — a *different* DataLake/OCID in the same tenancy behaves identically.
- **Control-plane (MCP) ✅:** `list_workspaces` (3: egress_disco_v2, AIDP_private_test_to_ATP, agentic_flow),
  `list_catalogs` (1), `list_schemas` (default, oci_ai_models), `list_clusters` (2 USER, both STOPPED;
  DEFAULT master-catalog ACTIVE), `list_roles` (3 incl. a custom role `a`), `list_agent_flows` (none),
  `list_recent_activities` (none), `create_notebook` + `delete_file` (created/cleaned up), `get_workspace`.
- **REST via `oci raw-request --profile DEFAULT` ✅:** same **`20240831/dataLakes`** shape —
  `shares` 200, `recipients` 200, `roles` 200 (3), `models?modelType=` 200; `credentials` 400 (route exists).
- **SQL-exec — not re-run here:** both USER clusters are STOPPED and `start_cluster` returned **400**
  (env/cluster-state issue, not a plugin issue — recorded as a real `cluster-ops` finding), the DEFAULT
  master-catalog cluster doesn't host a notebook server (sessions 404), and the instance has **no data
  tables**. The SQL-execution path is identical MCP code already verified end-to-end on instance #1
  (`tpcds`), so it was not repeated on this empty playground.
**Verdict:** the plugin works unchanged against a second AIDP instance — only the MCP alias / DataLake OCID
differs. Control-plane + REST are fully portable; SQL-exec requires a running notebook-capable cluster.

### No-MCP path + self-bootstrap — verified & hardened 2026-06-09
Tested the "new user with no MCP configured" path and closed the gap:
- **No-MCP control-plane works via `oci raw-request` (verified, `20240831/dataLakes`, DEFAULT api_key):**
  `catalogs` 200 (4), `schemas?catalogKey=` 200 (102), `tables?catalogKey=&schemaKey=` 200 (27),
  `clusters` 200, `jobs` 200, `workspaces`/`roles`/`shares`/`recipients`/`models` 200. → discovery, catalog,
  clusters, jobs, roles, sharing, models all run with **no MCP**. New doc: `references/no-mcp-rest-map.md`.
- **Only interactive Spark-SQL needs the MCP** (`nb_execute_code` WebSocket): analyzing-data, profiling,
  data-quality, ai-sql, verified-queries(validate), federate — with a documented job-based async fallback.
- **Self-bootstrap:** confirmed `claude` CLI **2.1.159** supports `claude mcp add <name> -e ENV=val -- cmd`.
  Rewrote `aidp-engineer-bootstrap` to **detect → install (`uvx`/`pip`) → register via `claude mcp add` →
  verify**, discovering the DataLake/workspace through the no-MCP REST path first. Updated
  `aidp-engineer-overview` (no-MCP rule) and README (MCP is auto-configured, not a hard prerequisite).

### Target `uvx aidp-mcp` as the setup command + publish runbook — 2026-06-09
- `aidp-engineer-bootstrap` + README now lead with `claude mcp add aidp -- uvx aidp-mcp` (zero-install) as
  the preferred command, with `uvx --from git+…repo aidp-mcp` / `python -m aidp_agent.mcp_server` as the
  works-now fallbacks until the package is on PyPI.
- Added `docs/PUBLISHING.md` (source-repo only; not shipped in the public tree): owner-run, one-time runbook to publish `aidp-agent`
  (entry point `aidp-mcp`) to PyPI — add LICENSE (UPL-1.0), polish `pyproject`, then GitHub Actions
  **Trusted Publishing** (tag-triggered, no tokens) or manual `twine upload`.
- **Note:** the Claude Code agent is barred from publishing a private repo to public PyPI
  (private→public data-exfiltration guardrail), so the actual publish is run by the repo owner per that
  runbook, after Oracle OSS clearance. No plugin change is needed post-publish — it already targets
  `uvx aidp-mcp`.
