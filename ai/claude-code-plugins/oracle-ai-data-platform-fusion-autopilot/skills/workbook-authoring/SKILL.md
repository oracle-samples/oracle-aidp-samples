---
name: workbook-authoring
description: "Author regenerated OAC workbook JSON with deterministic template-first generation, runtime semantic validation checks, MCP save-validation, and preview iteration."
---

# Workbook Authoring (Regenerated JSON, Runtime-Valid)

Use this skill to generate full workbook JSON that is both schema-shaped and plugin-runtime-valid for shipped template families.

## Scope

1. Two-mode authoring:
2. `regenerate_workbook` is the primary/default path (full deterministic regeneration).
3. `modify_existing` is additive and limited to supported scoped edits (filter-value edits, add filter-bar filter, title edits).
4. Multi-canvas + multi-viz workbooks are allowed and expected when the use case requires them; do not default to single-canvas/single-viz unless explicitly requested.
2. Disk-first output is mandatory.
3. Public API/token flows are out of scope.
4. Server sample workbook inspection is fallback-only.
5. Save-target mode is independent from generation mode: update requests should replace existing workbook by default.

## Preflight: OAC MCP connection (fail fast)

0. **Before anything else, confirm the `oac-mcp-server` tools are live** — this
   skill reads the OAC catalog through them (discovery, `describe_data`,
   save-validation). Probe once with a cheap `oracle_analytics-search_catalog`
   call (or check `claude mcp list` → `oac-mcp-server ✔ Connected`).
   - **If dead / unauthenticated:** STOP with an actionable message — do not
     proceed into catalog resolution (it would fail deep with an opaque error,
     or worse, read an empty result as "datasets don't exist"). Tell the user:
     from the customer project directory, run:
     ```bash
     env -u OAC_URL -u OAC_MCP_USER -u OAC_MCP_PASSWORD -u OAC_ADMIN_USER -u OAC_ADMIN_PASSWORD \
     aidp-fusion-autopilot dashboard mcp-setup \
       --connector-js <path-to-oac-mcp-connect.js>
     ```
     Then **restart/reconnect Claude Code** (`/mcp` → reconnect
     `oac-mcp-server`) so the tools activate, then re-invoke. The `env -u ...`
     wrapper lets the project `.env` win over any global shell OAC profile. If
     this workbook request is part of the Fusion Autopilot journey, write
     `.aidp/autopilot/resume.md` first with
     `skills/aidp-fusion-autopilot/write_resume_checkpoint.py`. MCP servers
     bind at session start; the connection cannot be established mid-session.
     (Autopilot front-loads this as Step 1b.)
   - A save-unavailable connection is fine (disk-first fallback below); a fully
     *dead* connection is not.

## Required MCP tools and capabilities

1. Mandatory:
2. `oracle_analytics_execute_logical_sql`
3. `oracle_analytics_describe_data`
4. Metadata discovery capability:
5. prefer `oracle_analytics-find_matching_datasources` when available for initial natural-language datasource matching
6. then use `oracle_analytics-search_catalog` for authoritative catalog resolution
7. fallback to `oracle_analytics-discover_data` only when newer discovery tools are unavailable
8. Modify-mode source acquisition requires one of:
9. content resource read capability for `content://` workbook JSON (via resources/read), or
10. user-provided local workbook JSON file path.
11. Save/export capabilities are optional and independent:
12. if `oracle_analytics-save_catalog_content` is unavailable, run disk-first generation + validation checks and return deterministic disk-only outcome
13. if save is available but `oracle_analytics-export_workbook` is unavailable, proceed with save + `viewUrl` and skip export
14. Export is opt-in by user intent; default behavior is save + return `viewUrl` without export

## Required local assets

Resolve `<WB_SKILL_ROOT>` as the directory containing the active workbook-authoring `SKILL.md`. Common package paths:
1. `.agents/skills/workbook-authoring` (Codex full zip layout)
2. `.claude/skills/workbook-authoring` (Claude full zip layout)
3. `skills/workbook-authoring` (local skill/plugin install layout)
4. fail fast if none exist

1. `<WB_SKILL_ROOT>/<TARGET_VERSION>/templates/template-index.json`
2. `<WB_SKILL_ROOT>/<TARGET_VERSION>/templates/*.json`
3. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/metadata-to-json-mapping.v1.json`
4. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/filter-profiling-contracts.v1.json`
5. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/calculation-contracts.v1.json`
6. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/version-field-catalog.json`
7. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/runtime-profile-contracts.v1.json`
8. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/semantic-validation-rules.v1.json`
9. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/plugin-type-aliases.v1.json`
10. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/viz-runtime-catalog.v1.json`
11. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/viz-resolution-profiles.v1.json`
12. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/presentation-polish-contracts.v1.json`
13. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/edit-operation-contracts.v1.json`
14. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/support-window.v1.json`
15. `<WB_SKILL_ROOT>/tools/runtime-validation-check.mjs`
16. `<WB_SKILL_ROOT>/tools/modify-workbook.mjs`
17. `<WB_SKILL_ROOT>/tools/regenerate-workbook.mjs`
18. `<WB_SKILL_ROOT>/tools/validate-requirements-trace.mjs`
19. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/regenerate-workbook-contract.v1.json`
20. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/regenerate-workbook-adapter-contract.v1.json`
21. `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/validation/schema-registry-profile.json`
22. `<WB_SKILL_ROOT>/<TARGET_VERSION>/schemas/workbook-schema-manifest.json`
23. `<WB_SKILL_ROOT>/<TARGET_VERSION>/schemas/*.js`
24. In multi-version installs, choose `<TARGET_VERSION>` from installed folders under `<WB_SKILL_ROOT>/` (minimum supported `26.01`).

## Required workflow

### 1) Parse intent

Capture:
1. visual type
2. business question
3. dimensions/measures/time fields
4. filters and layout expectations, including expected canvas count, viz distribution per canvas, and canvas names when known
5. whether user intent is update existing workbook vs create new workbook
6. whether detailed traces were explicitly requested (`trace`, `debug`, `diagnostics`, or `traceRequested=true`)

### 2) Save target resolution (required)

1. Keep generation policy fixed: regenerate complete workbook JSON from requirements.
2. Detect save capability first:
3. if `oracle_analytics-save_catalog_content` is unavailable, skip remote target resolution and continue with disk-only output.
2. Resolve save mode from user intent:
3. `replace_existing` when user asks to change/update an existing workbook (default for update intent).
4. `create_new` only when user explicitly asks for a new workbook/copy/variant.
5. Resolve target workbook by capability:
6. use `oracle_analytics-search_catalog` when available.
7. else use `oracle_analytics-discover_data` only for metadata discovery and do not attempt remote save target resolution.
6. In `replace_existing` mode:
7. require exactly one resolved workbook target (`id`).
8. if unresolved or ambiguous, fail fast with actionable message (do not silently create a new workbook).
9. In `create_new` mode:
10. create with `parentId` + `name`.
11. if same-name workbook already exists and user did not request duplicate/copy behavior, fail fast instead of creating duplicates.

### 3) Authoring mode router (required)

1. Route request to `regenerate_workbook` unless intent clearly requests a supported scoped modify operation.
2. `modify_existing` supports only:
3. edit existing filter operator/default/source values.
4. add filter-bar filter for an existing criteria column.
5. edit workbook/canvas/view titles; if title path is missing, create canonical path and then apply title update.
6. Route to `modify_existing` only when one source path is available:
7. catalog content read for `content://`, or
8. user-provided local workbook JSON file path.
9. If neither source path is available, fail with actionable guidance to download workbook JSON to disk and provide file path.
10. Unsupported modify intents must fail with actionable `not supported in modify_existing mode`.
11. Keep `regenerate_workbook` flow unchanged as primary path.
12. Execute primary regenerate flow through the bundled driver:

```bash
node <WB_SKILL_ROOT>/tools/regenerate-workbook.mjs --request <request.json> [--target-version "<YY.MM>"] [--detected-target-version "<YY.MM>"] [--output <workbook.json>]
```

> **File-location convention (where the JSONs live).** Author per workbook under
> a `workbooks/<name>/` directory **beside `bundle.yaml`** (mirrors
> `overlays/<name>/` and `profiles/`):
> - `workbooks/<name>/request.json` — the input spec (`analysisRequirements`).
> - `workbooks/<name>/workbook.json` — the generated, disk-first output.
> - `workbooks/<name>/<name>.redacted.json` — the only variant safe to commit.
>
> Default `--request workbooks/<name>/request.json` and
> `--output workbooks/<name>/workbook.json` when the caller doesn't specify
> paths. **Do not commit the raw `workbook.json` or any `*bound*` request** —
> they carry the OAC host / dataset UUID / session tokens; the bundle
> `.gitignore` ignores `workbooks/**/workbook*.json` + `workbooks/**/*bound*.json`
> (allowing `*.redacted.json`). Never write to `/tmp` (lost on reboot) or into
> `tests/live/` (that path is for skill *evidence*, not user work).

13. Build request payload to satisfy `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/regenerate-workbook-contract.v1.json`.
14. Build `adapterPayload` to satisfy `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/regenerate-workbook-adapter-contract.v1.json`.
15. For `compose_ootb`, include approved `analysisRequirements` before generation:
16. required gate: `brief -> discovery/profile -> requirements artifact -> explicit approval -> generate`.
17. `analysisRequirements.approval.status` must be `approved`.
18. Treat `analysisRequirements` as the single approved story/spec planning artifact. Do not introduce a separate YAML or compact-spec runtime.
19. `analysisShape` and `adapterPayload` are supporting execution inputs: `analysisShape` may be explicit, or it may be bootstrapped from `analysisRequirements.canvases`; `adapterPayload` remains the datasource/profile evidence.
20. `analysisRequirements` may include workbook intent (`workbook.name` / `description`), datasource aliases, reusable field aliases, reusable calculations, report/canvas filters, canvas layout intent, top-level navigation/data actions, and optional theme/presentation hints.
21. `analysisRequirements` must align with `analysisShape` canvas/view IDs when both are provided.
22. each compose view must include detailed planning (`purpose`, `grain`, `bindings`, `labels`, `filters`, `calculations`, and `sort`, optional `interactions`) or supported shorthand that normalizes to those fields.
23. supported view shorthand includes simple `type` / `viewType` / `visualization` values plus fields such as `x`, `y`, `category`, `value`, `metric`, `rows`, `columns`, `location`, and `filter`.
24. exact `pluginType`, explicit semantic-role `bindings`, exact calculations, interactions, and full data action metadata remain valid advanced planning; shorthand must not replace detailed planning when exact runtime shape is known.
25. unsupported shorthand or conflicting shorthand vs explicit bindings must fail fast; do not silently substitute another visualization.
26. each normalized `analysisRequirements.canvases[].views[].filters[]` entry must include:
27. `filterID`, `columnID`, `location`, `scope`, `operator`, `default`, `planningOutcome`.
28. compose preflight lint behavior is controlled by optional `composeFilterTolerance.mode`:
29. default `strict`: missing filter fields fail fast with JSON-pointer diagnostics (for example `/analysisRequirements/canvases/0/views/0/filters/0/scope`) before generation starts.
30. optional `tolerant`: missing `scope` auto-fills to `global`; missing `default` is derived from `adapterPayload.profiling.filterDecisionTrace.derivedDecisions` when possible; if derivation is not possible, fail fast with deterministic diagnostics.
31. regenerate output includes `composeFilterToleranceSummary` with mode and auto-fill telemetry.
32. minimal valid filter object shape:
33. `{"filterID":"flt_year","columnID":"dim_time_year","location":"filter_bar","scope":"global","operator":"in","default":["2025"],"planningOutcome":"applied"}`
34. Recommend (do not require) `analysisShape.canvases[].name` during initial generation so canvas titles are created correctly in one pass.
35. If canvas names are omitted, generation remains valid and titles can still be edited later.
36. Set `generationStrategy` explicitly for deterministic routing:
37. prefer `compose_ootb` by default for new workbook generation.
38. use `passthrough_bound` only when one of these is true:
39. the user explicitly asks to use an existing workbook and modify/preserve its current structure, or
40. the user asks to modify a previously `compose_ootb`-generated workbook using that workbook as bound input.
41. Do not let bound input presence alone (`adapterPayload.binding.boundWorkbookJson|boundWorkbookPath`) force passthrough in agent routing; set `generationStrategy="compose_ootb"` unless a passthrough condition above applies.
42. If `generationStrategy="auto"` is used, runtime routing is deterministic and will choose `passthrough_bound` whenever bound input exists; avoid `auto` for new-generation flows.
43. `passthrough_bound` requires bound workbook input and should not be blocked by OOTB topology limits.
44. `request.workbook.name` / `request.workbook.description` are save-layer metadata inputs only; do not persist them in workbook `content.json`.
45. Consume regenerate response `saveMetadata` for save-layer handoff (`name` / `description`) when save is attempted.
46. requirements trace uses severity split:
47. blocking mismatches (for example unresolved bindings/filters/calculations/placeholders) fail compose.
48. warnings (for example title caption mismatch) are returned in `requirementsTraceSummary.warnings` and do not block.

### 4) Modify-existing mode contract (when routed to modify_existing)

1. Source acquisition gate (required):
2. prefer catalog JSON read (`content://`) when available.
3. otherwise require user-provided local workbook JSON file path.
4. if neither source path is available, fail with actionable guidance (download workbook JSON and provide file path).
5. Resolve workbook target via catalog (`id`/path/name) for remote replace flows. If ambiguous, fail with candidate list and do not write.
6. Require explicit confirmation before every modify write.
7. Source mode defaults to catalog JSON read (`content://`).
8. Same-session fast-path bypass is allowed only when all are true:
9. same-session artifact path exists.
10. session artifact workbook id is present.
11. session artifact workbook id equals resolved target id.
12. On fast-path version/concurrency conflict, fail immediately (no auto-read fallback).
13. Apply deterministic mutator via `<WB_SKILL_ROOT>/tools/modify-workbook.mjs` with `--operation` set to a supported operation id from `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/edit-operation-contracts.v1.json`.
14. Add-filter operation requires `columnID` already present in `criteria.columns.children`; if omitted canvas scope defaults to all canvases.
15. Title-edit operation may create missing canonical title paths (`viewCaption.caption.text`) before applying updates.

### 5) Metadata discovery

1. Choose discovery method by tool capability:
2. use `oracle_analytics-find_matching_datasources` first for natural-language datasource shortlist when available.
3. use `oracle_analytics-search_catalog` to resolve and validate `datasets` and `subjectAreas` authoritatively.
4. otherwise use `oracle_analytics-discover_data` only as compatibility fallback when newer discovery tools are unavailable.
5. Build shortlist with `id`, `type`, `name`, `xsaExpr`, `viewUrl`.
6. Use `oracle_analytics-describe_data` in two phases:
7. `tablesOnly=true`
8. targeted `tableName`/`tableNames`
9. normalize field map: dimensions, measures, temporal, datatypes, defaults.

### 6) Filter profiling (required)

1. Run deterministic filter profiling probes from `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/filter-profiling-contracts.v1.json` using `oracle_analytics_execute_logical_sql`.
2. Profile candidate filter columns by class:
3. dimensions: top values + cardinality estimate
4. measures: min/max (+ bounded distribution sample where needed)
5. temporal: min/max + granularity hint
6. Enforce guardrails from the contract (deterministic sort/fetch, row/time limits, bounded retries).
7. If an individual profiling probe fails, continue with conservative fallback defaults and record fallback reason in filter decision trace.

### 7) Plugin resolution (required)

1. Resolve requested viz plugin type through `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/viz-resolution-profiles.v1.json`.
2. Determine `runtimeContractFamily`, `canonicalScaffoldTemplateId`, and `finalPluginType` from resolution profile.
3. Use `<WB_SKILL_ROOT>/<TARGET_VERSION>/model/plugin-type-aliases.v1.json` only as compatibility fallback metadata.
4. If requested plugin type is unmapped in resolution profiles, stop and report missing resolution contract before generation.

### 8) Profile handshake (version + runtime dialect)

1. Resolve target version from installed `<WB_SKILL_ROOT>/<YY.MM>/` bundles.
2. If `targetVersion` is provided, it must match an installed bundle folder.
3. If exactly one version bundle is installed and `targetVersion` is omitted, runtime auto-selects it.
4. If multiple bundles are installed and `targetVersion` is omitted, runtime honors `version-bundles.json` `defaultTargetVersion` when present. Without a manifest default, runtime prefers `26.07` when discovery is `search_catalog` and save is available; otherwise runtime prefers `26.05` when installed, else latest.
5. If a prior save validation payload contains version-mismatch signatures, pass `detectedTargetVersion` (or `--detected-target-version`) to keep retries sticky to the last accepted target.
6. Always report selected `targetVersion` in user-visible output.
7. Read support-window + runtime contracts from the selected version bundle for deterministic generation and validation-check behavior.
8. Resolve the internal schema target ID from the selected support-window entry for generation.
9. Resolve runtime profile/dialect from `runtime-profile-contracts.v1.json`.
10. Resolve runtime family from viz resolution profile (not by template guesswork).
11. Use default dialect; allow one fallback dialect only when known runtime errors indicate mismatch.

### 9) Template-first generation + calculations

1. Start from resolution profile `canonicalScaffoldTemplateId`; if user requested a different supported template, use that.
2. Keep template IDs stable; bind metadata into selected template.
3. Use `metadata-to-json-mapping.v1.json` for deterministic binding.
4. In `compose_ootb`, normalize supported `analysisRequirements` shorthand first, then consume per-view planning (`purpose`, `grain`, `bindings`, `labels`, `filters`, `calculations`, `sort`) before semantic fallback inference.
5. Rebind direct `criteria.columns[].columnFormula.expr.expression` values from scaffold defaults to target subject-area expressions using `adapterPayload.describe.columns`.
6. Post-bind, validate that every referenced `columnID` resolves in `criteria.columns` and each direct formula uses the selected subject-area token and a described target expression; fail fast if unresolved.
4. Build workbook structure to match requested analysis shape (single or multi-canvas; single or multi-viz per canvas).
5. Preserve requested plugin type by default when a requested plugin lock is provided; do not silently substitute viz types.
5. Do not invent ad-hoc JSON shapes.
5. Evaluate metric fit:
6. If base measures satisfy intent, bind base measures directly.
7. If no base measure fits, auto-gap-fill using workbook-local calculations from `calculation-contracts.v1.json`.
8. Supported calc types: `EXPRESSION`, `TEXT_GROUP`, `TIME_SERIES`.
9. Calculation columns must be persisted under `criteria.columns.children` with `userExpression=true`, `columnFormula.expr.expression`, and deterministic calc IDs.
10. Typed calculations (`TEXT_GROUP`/`TIME_SERIES`) must persist `criteria.criteriaConfig.settings.columnPropertyMap[columnID]` with `type`, `parentExpression`, and `options`.
11. Nested calc references must use `@calculation("<columnID>")` and reference calc columns that exist.
12. Any derived formula column that is not a direct source-column reference must be marked `userExpression=true` (do not emit non-editable derived formulas).
13. Persist formulas in OAC Logical SQL; translate source-workbook dialects first. Do not emit Tableau `COUNTD(...)`; use governed measures or `COUNT(DISTINCT ...)`. `POSITION(expr1 IN expr2)` is valid OAC syntax.
14. Filter mode defaults to `filter_bar`; use `filter_viz` only when explicitly requested.
15. For `filter_viz`, generated wiring must be runtime-complete:
16. `viewConfig.settings["viz:filter"].filterIDMap` / `parameterIDMap` must exist when relevant.
17. filter-viz row `logicalEdgeLayers` must match map keys (`columnID` for column controls, `name` for parameter controls).
18. every map value must resolve to a real filter control ID linked to the same filter-viz view (`location=filter_viz`, `filterViz=<viewName>`).
19. If a filter default uses `listParameterBinding`/`startParameterBinding`/`endParameterBinding`, declare the matching `parameters.settings[].name`; generated shared listbox bindings use multi-value text parameters.
20. Do not persist placeholder UI states such as `None` or `All` into `criteria.filter`; persist only real query defaults.
21. Data actions use top-level `dataActions[]`, not `dataActions.children`; BI Navigation and URL Navigation actions must follow the BI Tech source schema and bind context/anchor columns to criteria columns.
22. if `generationStrategyApplied=passthrough_bound`, do not auto-repair invalid filter wiring; fail fast with deterministic diagnostics.
23. Use profiling outputs to choose filter operators/default values and de-prioritize high-cardinality filter candidates unless explicitly requested.
20. If the user requests number-format behavior (for example currency, decimal places, grouping, abbreviation, or negative style), persist number-format config in plugin `viewConfig.settings`.
21. For chart-family settings, use `viewConfig.settings["viz:chart"]`:
22. use `numberFormat` for a shared/default chart formatter.
23. use per-field overrides with runtime key conventions (do not use dotted keys like `numberFormat.<fieldLabel>`):
24. for chart/table/pivot/autoviz/combo families, use `bidvtchart_number_format_<token>` and optional variants like `bidvtchart_number_format_<token>:::<columnID>` and `.tooltip`.
25. for performance tile families, use `numberFormat<token>` and optional variants like `numberFormat<token>:::<columnID>` and `.tooltip`.
26. Number-format payload must follow workbook number-format schema fields (for example `style`, `currency`, `useGrouping`, `minimumFractionDigits`, `maximumFractionDigits`, `useAbbreviation`, `abbreviationScale`, `negativeValuesStyle`, `currencyDisplay`).
27. Number-format enum values must be save-compatible: `abbreviationScale` is `off|on|thousand|million|billion|trillion` (`on` means automatic abbreviation), and `negativeValuesStyle` is `default|accounting|red|red_accounting` (`default` means minus-sign negatives).
28. Do not persist unsupported aliases such as `abbreviationScale:"auto"` or `negativeValuesStyle:"minus"`; normalize them to `on` and `default` before validation/save.
29. Do not place number-format payloads at workbook root or criteria nodes; keep them under the owning plugin `viewConfig.settings` path.
30. Presentation polish request block is optional and additive:
31. `presentationPolish.mode`: `auto | off | strict`
32. `presentationPolish.layoutTemplateHints`: optional `defaultArchetype`, `byCanvasID`, `byCanvasIndex`
33. `presentationPolish.titlePolicy`: optional `question_oriented | preserve_input`
34. Default polish behavior:
35. for `compose_ootb`, default `presentationPolish.mode=auto` (apply neutral_v2 layout/style polish + UX lint warnings)
36. for non-`compose_ootb`, default `presentationPolish.mode=auto` unless explicitly disabled
37. Polish applies deterministic layout archetype normalization (`executive_dashboard`, `filter_bar`, `filter_rail`, `content_grid`, `cover`) and keeps filter preference `filter_bar` unless user explicitly requests `filter_viz`.
38. In `strict` mode, severe UX lint findings must fail generation before save (do not bypass runtime checks).
39. Return `presentationPolishSummary` in normal output with `effectiveChangeCount`, `layoutChangeCount`, `styleChangeCount`, and `noOpReasons`; include `presentationPolishTrace` only when trace is explicitly requested.
40. Visualization intelligence request block is optional and advisory-only:
41. `visualizationIntelligence.mode`: `auto | off` (default `auto`)
42. `visualizationIntelligence.audienceProfile`: optional object (for example `role`, `targetLevel`) used to tune recommendation wording.
43. Visualization intelligence must never block generation/save; if scoring fails, return deterministic `dvIntelligenceSummary.status=scoring_unavailable` and continue.
44. Return `dvIntelligenceSummary` in normal output with `overallScore`, `audienceLevel`, `dimensionScores`, `recommendations`, `evidenceCoverage`, and `versionProfile`; include `dvIntelligenceTrace` only when trace is explicitly requested.
45. Best effort documentation link: if `dvIntelligenceSummary.references` is present, include the first reference URL when explaining scoring to end users.

### 10) Deterministic validation gate (required)

1. Schema validation check via bundled schemas/schema registry.
2. Semantic validation check via runtime contracts.
3. Calc-aware semantic checks must pass:
4. calc references resolve
5. calc dependency graph is acyclic
6. typed calc columnPropertyMap payload exists and is valid
7. calc columns are ordered deterministically before dependents
8. `regenerate-workbook.mjs` runs requirements-trace validation, canonicalization, and strict semantic validation check automatically.
9. Use manual check commands below only for direct debugging or deterministic patch/retry loops.
10. Run requirements-trace validation before runtime validation:

```bash
node <WB_SKILL_ROOT>/tools/validate-requirements-trace.mjs --request <trace-request.json>
```

11. Run canonicalization check (in-place) before strict validation:

```bash
node <WB_SKILL_ROOT>/tools/runtime-validation-check.mjs --input <workbook.json> [--target-version "<YY.MM>"] [--detected-target-version "<YY.MM>"] [--requested-plugin-type "<pluginType>"] --discovery-method "<search_catalog|discover_data>" --save-available "<true|false>" --export-available "<true|false>" [--export-requested "<true|false>"] [--version-selection-reason "<default_policy|user_requested_newer|required_newer_behavior|capability_heuristic_2607|capability_heuristic_2607_missing_fallback_latest|capability_heuristic_2605|capability_heuristic_2605_missing_fallback_latest|validation_fallback|session_sticky>"] --apply-known-patches --in-place
```

12. Then run strict semantic validation check:

```bash
node <WB_SKILL_ROOT>/tools/runtime-validation-check.mjs --input <workbook.json> [--target-version "<YY.MM>"] [--detected-target-version "<YY.MM>"] [--requested-plugin-type "<pluginType>"] --discovery-method "<search_catalog|discover_data>" --save-available "<true|false>" --export-available "<true|false>" [--export-requested "<true|false>"] [--version-selection-reason "<default_policy|user_requested_newer|required_newer_behavior|capability_heuristic_2607|capability_heuristic_2607_missing_fallback_latest|capability_heuristic_2605|capability_heuristic_2605_missing_fallback_latest|validation_fallback|session_sticky>"]
```

13. Canonicalization pass must seed runtime defaults (`parameters._version`, color/shape service domain scaffolding) to avoid first-open UI dirty rewrites.
14. Runtime validation check enforces schema acceptance and strips known-safe internal trace payload keys (`oracle.bi.tech.workbookAuthoringTrace`) before save attempts.
15. Must pass global checks and plugin-family checks before save attempts.
16. `--requested-plugin-type` is optional and should be used only for single-anchor-viz lock scenarios.
17. For multi-viz workbooks, omit `--requested-plugin-type`; the check still validates all plugin views and family/runtime invariants.
18. For modify mode, run validation check with explicit modify context:

```bash
node <WB_SKILL_ROOT>/tools/runtime-validation-check.mjs \
  --input <workbook.json> \
  --discovery-method "<search_catalog|discover_data>" \
  --save-available "<true|false>" \
  --export-available "<true|false>" \
  [--export-requested "<true|false>"] \
  [--version-selection-reason "<default_policy|user_requested_newer|required_newer_behavior|capability_heuristic_2607|capability_heuristic_2607_missing_fallback_latest|capability_heuristic_2605|capability_heuristic_2605_missing_fallback_latest|validation_fallback|session_sticky>"] \
  --authoring-mode "modify_existing" \
  --requested-operation "<operation_id_from_edit-operation-contracts.v1.json>" \
  --source-mode "<catalog_read|session_fast_path>" \
  --confirmation-state "confirmed" \
  --resolved-workbook-id "<targetId>"
```

18. Execution order is strict: do not parallelize `generate -> check -> save -> optional_export`.
19. Parallelism is allowed only for independent metadata discovery reads.
20. Runtime validation check capability inputs are mandatory and capability-driven: always pass `--discovery-method`, `--save-available`, and `--export-available` from runtime tool detection.
21. Pass optional `--export-requested` from explicit user intent for export; if omitted, validation check defaults `exportRequested=false`.
22. If orchestration has explicit version-target intent, pass optional `--version-selection-reason` (`default_policy|user_requested_newer|required_newer_behavior|capability_heuristic_2607|capability_heuristic_2607_missing_fallback_latest|capability_heuristic_2605|capability_heuristic_2605_missing_fallback_latest|validation_fallback|session_sticky`); otherwise validation check derives deterministic fallback reason. Optionally pass `--detected-target-version` when continuing from a prior save-validation mismatch.
23. If any required capability input is missing, runtime validation check must fail fast with deterministic `MISSING_EXECUTION_CAPABILITY_INPUT` and no save attempt.
24. If validation check returns `INPUT_ARTIFACT_NOT_READY`, treat it as an orchestration/readiness issue and rerun validation check after generation completes.
25. If filter-viz tiles show `No Data`, diagnose filter-viz map/linkage first:
26. verify `FILTER_VIZ_HAS_REQUIRED_MAPS`, `FILTER_VIZ_MAP_KEYS_MATCH_LDM_ROW_BINDINGS`, `FILTER_VIZ_MAP_VALUES_MATCH_EXISTING_FILTER_CONTROLS`, and `FILTER_VIZ_FILTERCONTROL_LINKAGE_CONSISTENT`.
27. resolve those invariants before attempting save/export retries.

### 11) Save loop (disk-first)

1. Save JSON locally first and return local path.
2. For workbook saves, payload must be wrapped as:
3. `content: { json: <workbook-json-object>, blobs?: [...] }`
4. Do not pass workbook root JSON directly as `content`.
5. Pre-save guard: if `type="workbooks"` and `content.json` is missing, fail fast locally before MCP call with an actionable message.
6. Keep workbook payload schema-clean: root `name` / `description` must not be present in `content.json`. Use save-layer metadata (`saveMetadata`) in save call arguments instead.
7. Example:

```js
oracle_analytics_save_catalog_content({
  type: "workbooks",
  parentId: "<folderId>",
  name: "FIFA18_2Canvas_Analyses_1_8",
  userApproved: true,
  content: {
    json: workbookJson
  }
});
```

8. Optional blobs:

```js
content: {
  json: workbookJson,
  blobs: [...]
}
```

9. Save mode contract:
10. `replace_existing` -> call `oracle_analytics_save_catalog_content` with workbook `id` (replace mode), not `parentId`.
11. `create_new` -> call `oracle_analytics_save_catalog_content` with `parentId` + `name` (create mode).
12. Prefer canonical wrapped payload (`content: { json: <workbook-json-object>, blobs?: [...] }`) for all saves.
13. Server/tool may auto-wrap root workbook objects in some environments, but do not rely on auto-wrap behavior in skill workflow.
14. Invalid save payload patterns (reject locally before save call):
15. file path references as `content` (for example `/tmp/workbook.json`)
16. `jsonPath`-style wrappers or path indirection for workbook content
17. non-JSON string content that cannot parse to a JSON object
18. Save transport fallback sequence (deterministic, max one retry):
19. Pre-save compaction guard: if the workbook JSON is already large enough that Codex MCP argument truncation/transport limits are likely, minify the same workbook object before the first MCP save call.
20. Compaction is whitespace-only unless generation knobs are explicitly being changed for a retry; parse original and minified forms and verify canonical deep equivalence (or matching canonical hash) before save.
21. Attempt 1 (canonical): `content: { json: <workbook-json-object>, blobs?: [...] }`, using the minified-equivalent object when compaction was applied.
22. If attempt 1 fails with payload-shape/transport signatures (for example `content string must be valid JSON object`, `workbook content must include json`, `Unable to parse the provided json`, serialization/size transport errors), run one compaction retry if it was not already applied.
23. If payload truncation/argument-size limits are suspected after canonical compaction, generate one reduced-size retry candidate with `numberFormatting.policy=none` and `presentationPolish.mode=off` while keeping the same analysis intent and topology.
24. Attempt 2 (interop fallback): retry with string form accepted by save tool (`content` as stringified JSON object, or `content.json` as stringified JSON when wrapper is required by caller environment).
25. Do not perform additional transport retries after attempt 2.
26. If save tool is available, execute the sequence above and stop on first success.
27. On save success, return saved target + `viewUrl` immediately.
28. If user explicitly requested export and export tool is available, run `oracle_analytics-export_workbook` and return preview artifact when ready.
29. If save tool is unavailable:
30. skip MCP save without failing generation,
31. return deterministic disk-only outcome and local artifact path.
32. If save succeeded but export tool is unavailable, skip export and still return saved target + `viewUrl`.
33. In no-save environments, `modify_existing` may return modified local JSON output but must not attempt server-side replace.
34. In trace mode only, emit `savePayloadMode` (`canonical_object|stringified_object`) and `compactionApplied` (`true|false`).
35. On save failure, return concrete payload-shape diagnostics including attempted payload mode, matched signature (if any), failure stage, and blocking constraint.

### 12) Deterministic remediation (one retry max)

If save/runtime returns known error signatures:
1. Map error -> patch action from `runtime-profile-contracts.v1.json`.
2. Apply deterministic patch set only.
3. Re-run semantic validation check.
4. Retry save once.

Example patch run:

```bash
node <WB_SKILL_ROOT>/tools/runtime-validation-check.mjs \
  --input <workbook.json> \
  --discovery-method "<search_catalog|discover_data>" \
  --save-available "<true|false>" \
  --export-available "<true|false>" \
  [--export-requested "<true|false>"] \
  [--version-selection-reason "<default_policy|user_requested_newer|required_newer_behavior|capability_heuristic_2607|capability_heuristic_2607_missing_fallback_latest|capability_heuristic_2605|capability_heuristic_2605_missing_fallback_latest|validation_fallback|session_sticky>"] \
  --runtime-error "<save-or-runtime-error-text>" \
  --apply-known-patches \
  --in-place
```

If second attempt fails:
1. return structured diagnostics
2. include contract gap summary
3. stop auto-retry

### 13) Server sample fallback policy

1. Do not inspect existing server workbooks by default.
2. Allowed only when validation checks pass and one remediation retry still fails runtime/visual acceptance.
3. When fallback is used, report the missing/insufficient contract rule.

### 14) Issue capture and feedback package (opt-in sharing)

When authoring fails, prepare a local feedback package that the user may choose to share.

Failure triggers:
1. validation check failure
2. save validation failure
3. export failure
4. saved workbook fails to open/render in UI
5. deterministic retry exhausted or unresolved contract gap

Feedback package contract (docs-level, agent-generated):
1. package folder name: `feedback-<YYYYMMDD-HHMMSS>-<short_slug>`
2. required files in both modes:
3. `ISSUE_REPORT.md`
4. `feedback_manifest.json`
5. `environment_context.json`
6. mode enum: `full | sanitized` (default `full` when mode is not specified)
7. `feedback_manifest.json` must include: `mode`, `targetVersion`, `authoringMode`, `failureStage`, `failureCode`, `failureMessage`, `includedFiles`, `omittedFiles`, `checksums`
8. deterministic `failureStage` values: `validation_check | save | export | ui_runtime | unknown`

Mode behavior:
1. `full`: include raw artifacts when available (request payload, generated workbook JSON, validation check outputs, save/export error payloads, trace diagnostics).
2. `sanitized`: include redacted summaries and field-level masks; do not include raw workbook JSON by default; include omitted-artifact inventory in manifest/report.

Sharing policy:
1. always create the package locally first
2. never auto-share
3. present a concise ready-to-share summary and ask whether the user wants to share
4. recommend `sanitized` for external sharing; `full` is acceptable for internal skill maintainers
5. if user declines sharing, keep package local and continue troubleshooting

## Runtime invariants to enforce

1. `table`: no column-edge layers; row carries dimension+measure.
2. `chart_autoviz`: `innerPluginType`, `measuresList` view entry, embedded `MeasureView_0`, hidden color measure layer, nested property additions.
3. `chart_autoviz` donut normalization: nested property additions must also include `min.<measure>` and `max.<measure>` entries.
4. `oracle.bi.tech.chart.scatter`: do not treat scatter as generic multi-measure autoviz. Scatter uses one embedded `MeasureView_0`; top-level and nested measure layers must carry `obitech-scatterchart#x` / `obitech-scatterchart#y` tags; nested X/Y layers must include the source-backed min/max/median/color `propertyAdditions`; and `MeasureView_1`-style nested measure scaffolding is blocked until backed by a runtime fixture.
5. `chart_combo_multilayer`: explicit combo `dataLayersInfo` in viewConfig + logicalDataModel, valid `activeDataLayer`, nested layer models per declared layer, and non-empty per-layer measure bindings.
6. `pivot`: requires row edge, column edge, and measures edge bindings.
7. `gantt`: requires row/category binding plus logical `item` edge with start/end tags (`obitech-gantt#start`, `obitech-gantt#end`).
8. `parallel_coordinates`: requires row binding plus at least two measure bindings on logical `col` edge.
9. `performance_tile`: requires logical measures binding and primary measure presence.
10. `map`: geography/category fields bind through `logicalEdges.detail`; metrics bind through map-specific color/size/layer roles, never execution column because OAC renders that role as `Unused`. Embedded map scaffolds use `dataLayersInfo` to mirror map logical edges, keep primary row/column edges unbound, carry detail on nested `MeasureView_0` row, and use `__EmbeddedVizDummyMeasureLink__` only as the embedded runtime measure marker.
11. `ui_control`: plugin view pluginType must be mapped; do not inject chart/table data-model assumptions.
12. `layouts.children[].layoutProps.customProps.text` must be a JSON-encoded object string; split layouts must include `oracle.bi.tech.layout.split.layoutMinSize`. Generate this field with `JSON.stringify`, not manual JSON text.
13. profile-required `reportConfig` service/settings nodes.
14. runtime-canonical defaults: `parameters._version`, missing filter parameter-binding definitions, color measure domains, and shape domains are pre-seeded so UI interaction does not rewrite workbook JSON immediately.
15. calculations:
16. `@calculation("<columnID>")` references must resolve to userExpression columns
17. no calc dependency cycles
18. typed calc entries require `criteria.criteriaConfig.settings.columnPropertyMap[columnID]`
19. no unsupported source-workbook formula dialect such as Tableau `COUNTD(...)`
20. filter parameter bindings resolve to `parameters.settings[].name`
21. data actions use top-level `dataActions[]` and source-schema BI Navigation/URL Navigation payloads
22. `criteria.filter` placeholder literals such as `None`/`All` are warning-level diagnostics

## Output contract

Default output mode is concise.

By default, return only:
1. local JSON file path (if produced)
2. saved workbook identifier/path (or explicit disk-only outcome when save is unavailable)
3. `viewUrl` on save success
4. selected `targetVersion` (always report this, including auto-selected single-bundle runs)
5. selected authoring mode (`regenerate_workbook` or `modify_existing`)
6. semantic validation check result summary
7. evidence/traceability summary: `evidenceLevel`, `requirementsTraceSummary`, `filterPlanningSummary`, `componentGraphSummary`, `fallbackUsageSummary`, `dvIntelligenceSummary`
8. generation strategy summary: `generationStrategyRequested`, `generationStrategyApplied`, `compositionCoverage`
9. export artifact summary only when export was explicitly requested and completed

Detailed traces are opt-in only. Include trace blocks only when user explicitly asks for `trace`, `debug`, `diagnostics`, or sets `traceRequested=true`.

When trace is requested, additionally return:
1. target execution capability block with `targetVersion`, `executionMode`, `reasonForVersionSelection`, `capabilitySource`, `saveToolDetected`, `exportToolDetected`, `discoveryMethod`, `saveAvailable`, `exportAvailable`, `exportRequested`
2. selected template ID + runtime family + dialect
3. strategy trace fields: `generationStrategyRequested`, `generationStrategyApplied`, `compositionCoverage`, `unsupportedTopologyReasons`
4. resolution trace block with `requestedPluginType`, `resolvedFamily`, `scaffoldTemplate`, `finalPluginType`, `fallbackUsed`, `reason`
5. filter decision trace block with `selectedFilterMode`, `queryIntents`, `probeResults`, `derivedDecisions`, `fallbackUsed`, `fallbackReason`
6. save target trace block with `requestedSaveIntent`, `resolvedSaveMode`, `resolvedWorkbookTarget`, `createBlockedByCollision`, `reason`
7. when in modify mode, modify trace block with `requestedOperation`, `resolvedWorkbookTarget`, `sourceMode`, `confirmationState`, `mutationsApplied`, `pathsChanged`, `fallbackUsed`, `fallbackReason`
8. keep filter/modify traces in tool/validation check output only; do not persist internal trace keys in workbook payload JSON
9. do not expose internal channel keys or internal `projectVersion` IDs in user-facing trace/output
10. include `dvIntelligenceTrace` only when trace is explicitly requested

On save success, return immediately:
1. saved workbook identifier/path
2. `viewUrl`

If export was explicitly requested and completed, return:
1. export artifact summary

On failure, return:
1. error text and available validation details
2. applied patch actions (if any)
3. contract gap report when unresolved
