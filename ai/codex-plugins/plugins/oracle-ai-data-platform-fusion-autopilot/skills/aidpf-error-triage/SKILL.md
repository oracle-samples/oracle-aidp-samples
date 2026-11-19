---
name: aidpf-error-triage
description: >-
  Read and explain AIDPF error codes, tracebacks, diagnostics, run IDs, datasets, layers, and variation failures, then route to the owning recovery Skill or command. Use whenever a bundle workflow fails or the user pastes failure evidence.
---

# aidpf-error-triage - route failures to the right recovery path

## Bundled CLI

Resolve `<plugin-root>` as the directory two levels above this `SKILL.md`.
On macOS/Linux, invoke `"<plugin-root>/bin/aidp-fusion-autopilot"`; on Windows,
invoke `"<plugin-root>\\bin\\aidp-fusion-autopilot.cmd"`. Command examples below
use `aidp-fusion-autopilot` as shorthand only—do not assume the plugin's `bin/`
directory is on `PATH`.


This skill is the read-only front door for failures. It turns pasted errors,
diagnostic artifacts, and failed-run summaries into a safe next step.

It does not fix anything itself. It identifies the failure class, names the
evidence, and routes to the skill or CLI command that owns the recovery.

## When to use

- User pastes output containing `AIDPF-*`.
- User gives `.aidp/diagnostics/<run_id>/...` or a run id.
- User says `validate`, `bootstrap`, `seed`, `incremental`, `dashboard`, OAC,
  dataset setup, or workbook authoring failed.
- User asks "what should I do with this error?"
- Autopilot hits an error and needs to route without guessing.

## When NOT to use

- Clean first-run setup with no failure -> `$aidp-fusion-autopilot`.
- Known single action with no error, such as "run seed" -> use that skill.
- Authoring new marts -> `$mart-author`.
- Creating workbooks from a verified dataset -> `$workbook-authoring`.
- Full error-code reference browsing -> read `docs/aidpf-error-codes.md`.

## Inputs to collect

Collect what is available, but do not block on everything:

- exact pasted error output,
- command that failed,
- active phase: validate, bootstrap, seed, incremental, dashboard, OAC dataset, workbook,
- run id,
- dataset/node id,
- layer,
- diagnostic artifact path or `.aidp/diagnostics/<run_id>/` directory,
- whether this is dev/sandbox or production.

Never ask the user to paste passwords, OAuth tokens, private keys, full OCIDs,
or full OAC connection payloads.

## Workflow

### 1. Extract the failure signal

Search the pasted text and any artifact paths for codes:

```text
AIDPF-\d{4}
```

If the user gives a diagnostics directory, list the files:

```bash
find .aidp/diagnostics/<run_id> -maxdepth 1 -type f -print
```

If the user gives a diagnostic JSON file, read it and extract:

- `errorCode`,
- `runId`,
- `tenant`,
- `datasetId` or `node`,
- `layer`,
- variation point name,
- missing columns,
- observed/live schema fields,
- companion logs such as `cluster_stdout.log`.

If multiple codes appear, triage in this order:

1. Fatal active code tied to the command's exit.
2. Diagnostic artifact code.
3. Runtime drift/gate code.
4. Pack validation aggregate such as `AIDPF-1036`, then its nested per-error code.
5. Historical or test-only code only if no active code exists.

If there is no `AIDPF-*` code, classify by symptom:

- OAC MCP server missing, disconnected, unauthenticated, or no tools -> run
  project-scoped `dashboard mcp-setup` plus resume checkpoint.
- `CredentialResolutionError` or missing Fusion password -> fix AIDP credential store / `aidp.config.yaml` secret names.
- `ResumeRunNotFoundError`, `ResumeRunNotResumableError`, or `ResumeBundleMismatchError` -> resume-specific guidance.
- Unknown traceback -> ask for the command and nearest error lines; do not guess a destructive fix.

### 2. Route by code family

Use this table for the first response. Keep it short, then include evidence and
the next command/skill.

| Code family | Meaning | Route |
|---|---|---|
| `AIDPF-1020` | Bootstrap operator identity missing. | Re-run `$aidp-fusion-bootstrap` with `--operator` or set `AIDP_OPERATOR` / `USER`. |
| `AIDPF-1030`, `1031`, `1033` | Missing content-pack profile or contentPack block. | Fix `bundle.yaml`; run `$aidp-fusion-bootstrap`. |
| `AIDPF-1034`, `1042`, `1043`, `1045`, `2081` | Invalid dataset/layer/scope or bundle node id. | Fix scope, use `content-pack info`, or `use-pack`; do not seed guessed nodes. |
| `AIDPF-1036`, `2000` to `2004`, `2030`, `2040`, `2041` | Pack or overlay validation failure. | Fix YAML/SQL/schema issue; if from a new mart overlay, return to `$mart-author`. |
| `AIDPF-2010`, `2011` | Bootstrap variation point unresolved. | `$medallion-author` with the diagnostic artifact, then bootstrap refresh. |
| `AIDPF-2012` | Bronze fingerprint drift from pinned profile. | `$fusion-drift-doctor` or `$aidp-fusion-bootstrap --refresh` after confirming drift is intended. |
| `AIDPF-2013` | Pre-extraction structural COA gate — `profile.chartOfAccounts` is MISSING/EMPTY/structurally INVALID while an in-scope node consumes a COA source (hard-block; NOT `allowUnprovableCOA`-eligible). | Configure a complete `profile.chartOfAccounts` (flat `balancing`/`costCenter`/`naturalAccountSegment`, or a nested `default`, optional numeric `byChart`) then re-run `$aidp-fusion-bootstrap`. Multi-chart tenants need `byChart` — route to `$medallion-author` if candidates are missing. |
| `AIDPF-2016`, `2017`, `2018` | COA plausibility — two roles share one physical column (2016); the `naturalAccountSegment` binding contradicts observed account types (2017); multiple active charts with only a singleton mapping and no `byChart` / acceptance (2018). | **First route: the metadata remediation loop** — `bootstrap --refresh --resolve-coa-from-metadata` (derives `byChart` from Fusion KFF segment qualifiers, Tier-B-verifies each arm against landed `gl_coa`, persists ONLY verified arms), read the ledger, then `run --resume <run_id>` (no `--mode`) when the phase exits 0. Read-only against Fusion, additive to the profile — routing it is never destructive. Falls back to `$medallion-author` (operator-authored arms) when the phase stops with 2021/2023, or for 2016 (a duplicated-role mapping is a config error metadata cannot fix). |
| `AIDPF-2021` | COA metadata resolution could not run — segment-qualifier source unreachable, credentials missing, principal lacks `FUN_GET_ENTERPRISE_STRUCTURES_REST_SERVICE_PRIV` (401/403), or the budget expired. Only raised when resolution was explicitly requested. | Grant the privilege / fix `FUSION_BICC_USER`+`FUSION_BICC_PASSWORD`, or pin the resource via `coa metadata-probe` + `fusion.coaMetadata`. Fall back to operator-authored `byChart` via `$medallion-author`. |
| `AIDPF-2022` | **Warn-only.** A metadata-derived arm was REJECTED and NOT persisted (failed Tier-B 2017, duplicated a role column 2016, or named an invalid/out-of-domain segment 2019/2042/5001). | Read `AIDPF-2022__coa-metadata.json`; author that chart's arm manually via `$medallion-author`. A rejected arm is proof the derivation is wrong for that chart — never a transient to retry. An AIDPF-2022 detail for an INACTIVE chart coexists with a phase exit 0 and does not block a resume. |
| `AIDPF-2023` | After metadata resolution, one or more ACTIVE in-scope charts still have no `byChart` arm (and no accepted shared layout). Verified arms WERE persisted (monotonic); the phase exits non-zero because the remediation objective was not met. | Resolve the listed charts (privilege for the metadata source, or `$medallion-author` arms), or `--accept-singleton-coa` only when a shared layout is genuinely true. The post-extraction AIDPF-2018 gate stays the enforcer for the next seed. |
| `AIDPF-2074` | COA correctness PROBE could not EXECUTE (e.g. constrained Spark session) — correctness UNPROVEN; blocks by default. | Fix the session so probes run, or set `contentPack.allowUnprovableCOA: true` to proceed with a logged WARN (correctness then rests on the per-node backstop). A real COA VIOLATION still hard-blocks regardless. |
| `AIDPF-2042`, `2043`, `2044`, `2046`, `2072`, `4070`, `4071` | Source, PVO, or runtime schema drift/gate. | `$fusion-drift-doctor`; it may route to bootstrap refresh, `$medallion-author`, or investigate. A `4071`/`2042` caused by an *added* required column → `$medallion-author` (fix the add / extend the source); a normally-required column *legitimately absent* on this tenant → `$medallion-author` to relax it via `relaxRequiredColumns`. |
| `AIDPF-2062`, `2063` | Bronze `requiredColumns` overlay guard — same-id file dropped a required column (2062), or `relaxRequiredColumns` named a non-base column (2063). | `$medallion-author`; remove via a `relaxRequiredColumns` block override (with a `reason`), and only relax a column the base actually requires. |
| `AIDPF-2084` | Declared-inputs gate — a silver/gold SQL reads an upstream column not declared in `requiredColumns` (or a `SELECT *` / `<alias>.*` from an upstream). | `$mart-author` (or `$medallion-author` for an overlay node): declare the column in `requiredColumns[<source>]` (and add it to the upstream `outputSchema` if absent), or replace the wildcard with an explicit alias-qualified projection. |
| `AIDPF-2085` | Declared-inputs **warn-only** — a bare unqualified column matches an upstream `outputSchema`. | Not blocking. Qualify the column with its table alias so AIDPF-2084 can verify it. |
| `AIDPF-2047`, `2048`, `2049` | Cluster bootstrap dispatch/probe failure. | `$aidp-fusion-bootstrap`; inspect diagnostic JSON and `cluster_stdout.log` if present. |
| `AIDPF-2071` | Bronze readiness failed for silver/gold. | Seed/repair required bronze through `$aidp-fusion-seed`; do not run mart-only refresh until bronze exists. |
| `AIDPF-2092` | Bronze cursor and target/state mismatch. | Repair state/target alignment; usually inspect status before incremental. |
| `AIDPF-2093` | Connector value/type mismatch — the `aidataplatform` connector produced values that do not match its OWN declared column type for a PVO (e.g. Long under decimal(38,0)); every encode-forcing op fails while pruned counts pass. | **First route:** `bronze diagnose-encode --dataset <id>` (read-only bisection; names the column + probes its runtime type), then add the printed `schemaPatches` entry on the bundle dataset and re-run. **Never route to excluding the dataset first** — the patch keeps full-width ingestion. If diagnose reports "no uniform runtime type", the column cannot be patched: file the connector defect. |
| `AIDPF-2094` | schemaPatches cast-integrity violation — casting a patched column back to its declared type changed data (null introduced / round-trip mismatch); the node failed BEFORE the write, nothing landed. | The configured patch type is wrong for this column's values: re-run `bronze diagnose-encode --dataset <id>` and set the patch to the probed runtime type. A "no uniform type" result means the vendor connector fix is the only cure — do not force a patch. |
| `AIDPF-4022` | Durable pre-execution run manifest failed to commit (fresh run), or a resume found the manifest row malformed / unknown-version / missing a required field. | Nothing was extracted on a commit failure — re-run `--mode seed` via `$aidp-fusion-seed`. A malformed manifest is non-resumable; start a fresh seed (do not `--resume`). |
| `AIDPF-4023` | Run reconciliation — the AIDP job/notebook reported SUCCESS but the run **aborted** or its completion is **unproven** (failed/aborted step, reserved gate step, or an expected node with no terminal state row). The job status is not the run's verdict. | Read the printed `RUN VERDICT` block and its named `AIDPF-*` codes; remediate those, then `run --resume <run_id>` (no `--mode` — the resume adopts the run's recorded mode). Never report a seed complete on job status alone. |
| `AIDPF-4040` | Plan-hash drift on resume/incremental. | `$fusion-drift-doctor`; if intentional, scoped re-seed changed node or documented repin in dev only. |
| `AIDPF-4060`, `4061` | State commit or watermark regression. | Stop; inspect state-table write/order issue before retrying. |
| `AIDPF-5001`, `5002`, `5010`, `5011`, `5013`, `5014` | SQL renderer or builtin dispatch issue. | Fix SQL/template/profile value; route to `$mart-author` if a new overlay caused it. |
| `AIDPF-5003` | Variation point unresolved during rendering. | `$aidp-fusion-bootstrap`; if candidate missing, `$medallion-author`. |
| `AIDPF-7001` to `7005`, `8002` | Dashboard validation/security issue. | Fix dashboard descriptor or workbook requirements; remove high-PII exposure. |
| `AIDPF-8010`, `8011` | Quality test failed or unsupported. | Inspect failed quality rule; fix data/node logic or accept deferred rule intentionally. |
| `AIDPF-9999` | Test-only invalid code. | Treat as malformed diagnostics outside tests. |

**No AIDPF code — classify by the DEEPEST cause, never by absence.** A
missing `AIDPF-*` code alone proves nothing: it could be transient
infrastructure OR an uncoded plugin/config bug (`AttributeError`,
`ValueError`, an import failure in a cell). First extract the deepest
cause — the executed notebook's cell traceback / the `Caused by:` chain —
then branch:

- **POSITIVE transient signature present** (any of: `HikariPool …
  Connection is not available`, `MetaException`, connection/read
  **timeout** wording, `Connection refused/reset`, executor-lost /
  node-decommission messages, laptop-side
  `poll_warn err=ReadTimeout|ConnectionError`) → transient
  infrastructure:
  1. **Change NOTHING** — never edit the bundle/profile/patches over a
     transient.
  2. Mid-run node failure (verdict `ABORTED`, other nodes green) →
     `run --resume <run_id>` (no `--mode`) — re-runs only the failed
     nodes.
  3. Pre-run/cell failure (`DISPATCH_RUN_FAILED`, exit 2) → retry the
     SAME command once.
  4. Repeats after two attempts → cluster/metastore health problem;
     escalate to the AIDP console, do not keep retrying.
- **No transient signature** (e.g. a Python `AttributeError`/`TypeError`/
  `ValueError`, a `ModuleNotFoundError`, an assertion) → **unclassified
  failure**: do NOT retry and do NOT call it cluster health. Fetch and
  report the full cell exception (executed notebook / job run output),
  then triage the actual cause — an uncoded plugin exception is a bug
  report, not weather.

(Live precedent for the transient branch: an `ap_payments` saveAsTable
metastore timeout — that node's first failure in five runs — healed by one
resume; an early `o272.sql` HikariPool timeout healed by re-running the
same resume. Both carried the HikariPool/MetaException signature in the
deepest cause.)

For codes not listed here, consult `docs/aidpf-error-codes.md` and state that
the route is based on the reference table.

### 3. Read diagnostics when available

When the route names a downstream skill, pass concrete artifacts:

```text
run id:
diagnostic file:
dataset/node:
layer:
variation point:
missing columns:
next skill:
```

Examples:

- `AIDPF-2010__invoice_currency_code.json` -> `$medallion-author`.
- `AIDPF-2072.json` or `AIDPF-4071__<node>.json` -> `$fusion-drift-doctor`.
- `AIDPF-2048.json` plus `cluster_stdout.log` -> `$aidp-fusion-bootstrap`.

Do not synthesize missing diagnostic context. If an expected artifact is absent,
say what file is needed or ask for the command output.

### 4. Produce the triage response

Use this format:

```text
code: AIDPF-2072
phase: incremental
meaning: live Fusion PVO schema drifted from the pinned profile
evidence: .aidp/diagnostics/<run_id>/AIDPF-2072.json
route: $fusion-drift-doctor
next: diagnose live PVO drift; likely bootstrap --refresh or $medallion-author
do not: bypass drift gates or run destructive seed
```

For OAC MCP disconnect with no AIDPF code:

```text
code: none
phase: OAC MCP
meaning: MCP connection is unavailable; this is not proof that datasets are absent
route: from the customer project directory run:
  env -u OAC_URL -u OAC_MCP_USER -u OAC_MCP_PASSWORD -u OAC_ADMIN_USER -u OAC_ADMIN_PASSWORD \
  aidp-fusion-autopilot dashboard mcp-setup --connector-js <path>
  then write .aidp/autopilot/resume.md and reconnect Codex
next: resume from .aidp/autopilot/resume.md and re-probe OAC
```

### 5. Guardrails

- Stay read-only. Do not edit files, run bootstrap, seed, incremental, or
  workbook save from this skill.
- Never recommend destructive seed as the first recovery unless the owner skill
  has proven it is the right fix and the user confirms.
- Never recommend `--force-fingerprint-skip` outside dev/break-glass.
- Never hand-write `profiles/` or `evidence/`.
- Never route `AIDPF-2010` / `AIDPF-2011` to `$mart-author`; use `$medallion-author`.
- Never treat disconnected OAC MCP as an empty OAC catalog.
- **Never treat an AIDP job status of SUCCESS as a successful run.** The
  verdict is the `RUN VERDICT` block (`run_reconcile` codes / the
  `__run_outcome__` state row); a SUCCESS job with an `ABORTED`/`UNPROVEN`
  verdict is a FAILED run (AIDPF-4023).
- **Never recommend `--accept-singleton-coa` when AIDPF-2017 evidence is
  present** — the contradiction already disproves a shared layout; routing
  it would persist a mapping the data has falsified.
- Metadata resolution (`--resolve-coa-from-metadata`) is read-only against
  Fusion and additive to the profile — proposing it is not a destructive
  recommendation. (This skill still only ROUTES it; it never runs it.)
- Always name the downstream skill or command and the evidence it should read.

## Skill family

- Front door for failures from `$aidp-fusion-autopilot`.
- Routes config gaps to `$aidp-fusion-config`.
- Routes bootstrap/profile issues to `$aidp-fusion-bootstrap`.
- Routes tenant variation gaps to `$medallion-author`.
- Routes runtime drift gates to `$fusion-drift-doctor`.
- Routes run-mode work to `$aidp-fusion-seed` or `$aidp-fusion-incremental`.
- Routes OAC dataset/workbook issues to `$oac-dataset-setup` or `$workbook-authoring`.
