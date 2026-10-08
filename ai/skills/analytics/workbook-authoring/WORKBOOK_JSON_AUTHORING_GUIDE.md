# Workbook JSON Authoring Guide (v1.2 Runtime-Valid)

This guide defines deterministic workbook authoring that targets runtime-valid plugin contracts, not just schema-valid JSON.

## 1) Required source artifacts

Resolve `<PROFILE_ROOT>` from `compatibility-profiles.json` as `compatibility-profiles/<PROFILE_ID>`.

1. `<PROFILE_ROOT>/model/metadata-to-json-mapping.v1.json`
2. `<PROFILE_ROOT>/model/filter-profiling-contracts.v1.json`
3. `<PROFILE_ROOT>/model/calculation-contracts.v1.json`
4. `<PROFILE_ROOT>/model/version-field-catalog.json`
5. `<PROFILE_ROOT>/model/runtime-profile-contracts.v1.json`
6. `<PROFILE_ROOT>/model/semantic-validation-rules.v1.json`
7. `<PROFILE_ROOT>/model/plugin-type-aliases.v1.json`
8. `<PROFILE_ROOT>/model/viz-runtime-catalog.v1.json`
9. `<PROFILE_ROOT>/model/viz-resolution-profiles.v1.json`
10. `<PROFILE_ROOT>/model/presentation-polish-contracts.v1.json`
11. `<PROFILE_ROOT>/model/edit-operation-contracts.v1.json`
12. `<PROFILE_ROOT>/model/support-window.v1.json`
13. `<PROFILE_ROOT>/templates/template-index.json`
14. `tools/runtime-validation-check.mjs`
15. `tools/modify-workbook.mjs`
16. `tools/regenerate-workbook.mjs`
17. `tools/validate-requirements-trace.mjs`
18. `<PROFILE_ROOT>/model/regenerate-workbook-contract.v1.json`
19. `<PROFILE_ROOT>/model/regenerate-workbook-adapter-contract.v1.json`
20. `tools/compatibility-profile-utils.mjs`

## 2) Canonical generation order

1. Parse intent.
2. Route to authoring mode:
3. `generate_fresh` (primary default), or
4. `modify_existing` (limited `edit_filter_values_or_operator` / `add_filter_bar_filter` / `edit_titles`).
5. Resolve save target mode (`replace_existing` vs `create_new`) before generation/mutation.
6. For `modify_existing`, resolve exact workbook target (`id`) and require explicit confirmation.
7. Metadata discovery (use `find_matching_datasources` first when available, then `search_catalog` for authoritative catalog resolution, use `discover_data` only as compatibility fallback when newer discovery tools are unavailable, then two-phase `describe_data`).
8. Filter profiling using `oracle_analytics_execute_oac_ansi_sql` when available, with `oracle_analytics_execute_logical_sql` as the availability and semantic fallback, using deterministic probes from `filter-profiling-contracts.v1.json`.
9. Requirements gate for `compose_ootb`: build requirement artifact, require explicit approval, and persist as `analysisRequirements`.
10. Resolve plugin profile (`pluginType -> runtimeContractFamily + canonicalScaffoldTemplateId + finalPluginType`).
11. Compatibility handshake (server project-version range + runtime dialect).
12. Template selection by canonical scaffold template ID.
13. Deterministic metadata binding (fresh mode) or deterministic mutate operation (`modify-workbook.mjs`) in modify mode.
14. Metric-fit evaluation + workbook-local calc synthesis when required.
15. Presentation polish phase (layout/title/style normalization + UX lint) for `compose_ootb` by default.
16. Requirements trace validation (`validate-requirements-trace.mjs`).
17. Schema validation check.
18. Semantic runtime validation check.
19. Local save.
20. MCP save (replace by `id` for updates, create by `parentId`+`name` for explicit new copies).
21. Immediate `viewUrl` publication on save success.
22. Optional export preview (only when explicitly requested).
23. One deterministic remediation retry for known runtime failures.
24. Run `generate/mutate -> validation check -> save -> optional_export` sequentially (no parallel execution between these steps).
25. Parallel execution is only acceptable for independent metadata discovery reads.

Use the bundled canonical regenerate driver as the default implementation path:

```bash
node .workbook-authoring/tools/regenerate-workbook.mjs --request <request.json> [--compatibility-target "<auto|legacy|standard|baseline|current|pv-N>"] [--server-project-version "<N>"] [--server-info-file <server-info.json>] [--target-version "<deprecated YY.MM alias>"] [--output <workbook.json>]
```

The request payload must satisfy `<PROFILE_ROOT>/model/regenerate-workbook-contract.v1.json`, and `adapterPayload` must satisfy `<PROFILE_ROOT>/model/regenerate-workbook-adapter-contract.v1.json`.
When request includes `workbook.name` / `workbook.description`, treat them as save metadata only. They must not be persisted at workbook JSON root inside `content.json`; consume driver response `saveMetadata` for save-layer handoff.
Recommend providing `analysisShape.canvases[].name` during initial generation when canvas labels are known. This field is optional and omitted names do not block generation.
For `compose_ootb`, `analysisRequirements` is required and must be explicitly approved. `analysisShape.canvases[].views[]` must include `purpose`, `grain`, `bindings`, `labels`, `filters`, `calculations`, and `sort`.
Field aliases and binding objects may include a source-backed `aggregation` and a structured `format` object. Direct-source aggregation must match `describe_data` or use `default` / `none`; conflicting overrides and free-form formatting strings fail fast. Direct sort entries persist as logical edge-layer `columnSort` metadata.
Compact chart shorthand supports primary and secondary measures only; additional measures fail instead of being truncated. Non-empty per-view `interactions` fail until source-backed materialization exists; use top-level `actions` / `dataActions` for supported navigation.

For a line chart, `x` is classified using verified `describe_data` metadata, not a field-name guess or a planning type hint. Numeric/text attributes (including nonaggregated year) use the `line_category` scaffold and `dimension.primary`; temporal fields retain `line_time` and `temporal.primary`. Both preserve two ordered governed measures and direct source expressions. Categorical lines use the hidden measure dimension for series coloring without adding a continuous-measure color binding. Explicit `time`/`date` or `temporal.primary` still requires temporal metadata. If the selected installed profile lacks `line_category`, stop with the missing-scaffold diagnostic; do not change the source datatype, replace the chart with bars, or discard a measure. Archived profiles are unchanged.
For `compose_ootb`, each `analysisRequirements.canvases[].views[].filters[]` object must include:
1. `filterID`
2. `columnID`
3. `location`
4. `scope`
5. `operator`
6. `default`
7. `planningOutcome`
Compose preflight lint runs before generation and supports optional `composeFilterTolerance.mode`:
1. `strict` (default): missing required filter fields fail fast with JSON-pointer diagnostics.
2. `tolerant`: missing `scope` auto-fills to `global`; missing `default` is derived from `adapterPayload.profiling.filterDecisionTrace.derivedDecisions` when available, otherwise fails fast with deterministic diagnostics.
Regenerate output includes `composeFilterToleranceSummary`.
Minimal valid filter object example:
`{"filterID":"flt_year","columnID":"dim_time_year","location":"filter_bar","scope":"global","operator":"in","default":["2025"],"planningOutcome":"applied"}`
Applied filters must persist in source-backed `filterControlCollections` with matching expression, operator, defaults, and scope.
If filter materialization cannot produce schema-supported wiring, stop with diagnostics; do not remove an applied filter to produce a saveable workbook. View-scoped collections are linked from the plugin view, not a layout cell.

For the current master target (26.10/26.11/26.12), authoring may explicitly request chart value-axis trellis scope, date-part number formats (`dateDisplay` month/day plus `dateStyle`), gauge range-label number format, and bounded value-based color domains. These controls are opt-in and are rejected for earlier compatibility targets or incompatible visualization families.
`generationStrategy` controls hybrid regenerate behavior:
1. `auto` (default): use passthrough when bound workbook input exists, else compose OOTB.
2. `compose_ootb`: deterministic OOTB topology composition from `analysisShape` for supported 80/20 multi-canvas/multi-viz cases.
3. `passthrough_bound`: preserve bound workbook topology as advanced/custom escape hatch and enforce only base contract + validation check.
`presentationPolish` controls additive presentation normalization:
1. `mode`: `auto | off | strict`
2. `layoutTemplateHints`: optional `defaultArchetype`, `byCanvasID`, `byCanvasIndex`, `defaultRegisteredTemplate`, `registeredTemplateByCanvasID`, `registeredTemplateByCanvasIndex`
3. `titlePolicy`: optional `question_oriented | preserve_input`
4. defaults: `compose_ootb -> auto`, non-`compose_ootb -> auto` unless explicitly disabled
5. OAC system registered layout templates are supported as source-backed layout intent/zones: `bitech.layoutTemplate.filterLeft` and `bitech.layoutTemplate.filterTop`.
6. Use `analysisRequirements.canvases[].layout.templateID` for explicit per-canvas registered template selection; registered templates are not auto-applied as the default layout engine.
7. Generated dashboard JSON stays compact by default: performance tiles are KPI strip cells with compact NG tile typography, and three/four-view canvases should fit the visible workbook surface unless long scrolling is intentional.
8. `strict` mode fails generation on severe UX lint findings before save.
9. `presentationPolishSummary` must include effect telemetry fields: `effectiveChangeCount`, `layoutChangeCount`, `styleChangeCount`, and `noOpReasons`.
`visualizationIntelligence` controls advisory-only DV scoring:
1. `mode`: `auto | off` (default `auto`)
2. `audienceProfile`: optional object (for example `role`, `targetLevel`) used to tune recommendation wording.
3. scoring failures are non-blocking; emit `dvIntelligenceSummary.status=scoring_unavailable` and continue.
Regenerate response telemetry must include `evidenceLevel`, `requirementsTraceSummary`, `planningFilterMaterializationSummary`, `planningConsumptionSummary`, `filterPlanningSummary`, `componentGraphSummary`, `fallbackUsageSummary`, and `dvIntelligenceSummary`.
Requirements-trace severity model:
1. missing requested bindings, aggregation, formats, filters, sorts, calculations, or titles fail compose (`REQUIREMENTS_TRACE_VALIDATION_FAILED`).
2. warnings are advisory-only and returned in `requirementsTraceSummary.warnings`.
3. `planningConsumptionSummary.valid` must be true with `unconsumedCount=0` before runtime validation and save.

## 2.1) Modify-v1 operation contract

1. Source of truth: `<PROFILE_ROOT>/model/edit-operation-contracts.v1.json`.
2. Supported operations only:
3. `edit_filter_values_or_operator` edits existing filter operator/default/source values.
4. `add_filter_bar_filter` adds a filter-bar filter for an existing criteria column.
5. `edit_titles` edits workbook/canvas/view titles and can create the canonical title path (`viewCaption.caption.text`) when absent.
6. Source acquisition policy:
7. default `catalog_read`.
8. `session_fast_path` only when same-session artifact exists and workbook id exactly matches resolved target id.
9. On fast-path concurrency/version conflict, fail immediately (no auto catalog read fallback).
10. Every modify write requires explicit confirmation.
11. Modify output must include `modifyTrace` in tool/validation check output and must not persist internal trace keys under `reportConfig.settings`.

## 3) Calculation contract (workbook-local, auto gap fill)

1. Calculation source of truth is `<PROFILE_ROOT>/model/calculation-contracts.v1.json`.
2. Supported calc types:
3. `EXPRESSION`
4. `TEXT_GROUP`
5. `TIME_SERIES`
6. Default policy is auto gap fill: if discovered base measures do not satisfy requested metric intent, synthesize workbook-local calculation columns.
7. Required calculation persistence:
8. add calc columns to `criteria.columns.children` with `userExpression=true`
9. set `columnFormula.expr.expression` and `columnHeading.caption.text`
10. for typed calcs (`TEXT_GROUP`/`TIME_SERIES`), persist `criteria.criteriaConfig.settings.columnPropertyMap[columnID]` with `type`, `parentExpression`, and type-specific `options`
11. nested calc references must use `@calculation("<columnID>")`
12. calc dependency graph must be acyclic and ordered before dependent calcs
13. non-direct derived formulas are never emitted as plain regular columns; they must be `userExpression=true` so they remain editable in My Calculations
14. formulas must be OAC Logical SQL, not source-workbook dialect syntax
15. do not emit Tableau `COUNTD(...)`; use governed measures when available or `COUNT(DISTINCT ...)`
16. `POSITION(expr1 IN expr2)` is valid OAC syntax and must not be rejected

## 4) Filter profiling contract (required)

1. Profiling source of truth is `<PROFILE_ROOT>/model/filter-profiling-contracts.v1.json`.
2. Select the profiling tool from the actual MCP tool inventory; do not infer availability from release or project version.
3. Prefer `oracle_analytics_execute_oac_ansi_sql`; when it is unavailable, use `oracle_analytics_execute_logical_sql` directly.
4. Use Logical SQL with `OVERRIDEAGGR` for governed non-complex measure statistics; skip unsafe statistics for complex measures and record fallback.
5. Probe classes:
6. dimensions: top values + cardinality estimate.
7. measures: min/max + optional bounded distribution sample.
8. temporal: min/max + granularity hint.
9. Guardrails are mandatory: deterministic ordering, explicit fetch limits, bounded row/time thresholds, bounded retries.
10. If neither query tool is available, continue with conservative defaults and record the unavailable capability in the filter decision trace.
11. Probe-level failures do not stop generation; they trigger conservative fallback with explicit trace reason.
12. Default filter policy remains filter-bar-first with moderate auto-filter selection.
13. High-cardinality candidates are de-prioritized unless explicitly requested.
11. Trace-mode output fields (include only when user explicitly requests `trace`, `debug`, `diagnostics`, or `traceRequested=true`):
12. `selectedFilterMode`, `queryIntents`, `probeResults`, `derivedDecisions`, `fallbackUsed`, `fallbackReason`.

## 5) Compatibility handshake

Connected execution requires an explicit handoff, not merely a preceding MCP call. Save the complete discovery response as a task-local `server-info.json`; pass `--server-info-file <server-info.json>` to generation and every standalone validation, modify-validation, and remediation command. The generator also accepts request `serverInfo`, forwards it to internal checks, and returns `compatibilityValidationSummary.serverInfoFile` for explicit reuse. Do not auto-load stale neighboring sidecars.

Accepted inputs are the decoded `oracleAnalytics` payload or a successful MCP result with exactly one `content` block of `type: "text"` containing that payload as JSON (`isError` false or absent). The parser unwraps this transport shape before the same strict compatibility checks; generated sidecars retain normalized workbook capabilities. For other client wrappers, supply the complete decoded payload. Error results, malformed text, nested wrappers, and multiple/mixed representations are rejected, not downgraded to version-only compatibility.

If discovery is genuinely unavailable, use request `serverInfoUnavailableReason` or `--server-info-unavailable-reason "<actual non-secret reason>"` instead. A missing connected handoff fails with `MISSING_SERVER_INFO_HANDOFF` before writing; changing targets or hiding connectivity is not a remedy. Malformed/conflicting discovery remains an error. Old-server version-only responses and legacy `--server-project-version` integrations remain supported; never reduce a complete response to its project version. Offline disk-only generation needs no context.

Normal output includes `compatibilityValidationSummary.mode` (`server_capabilities`, `project_version_only`, `explicit_fallback`, or `offline`), `serverComponentValidation`, `fallbackReason`, and `serverInfoFile`. Before save, distinguish locally valid JSON from validation against advertised server components. In the examples below, bracketed server context is optional only for offline use or an explicitly declared unavailable-discovery fallback.

1. Call `oracle_analytics-get_server_info` once and retain the complete response in request `serverInfo` or `--server-info-file`. The driver uses project version to find a preferred profile and `schemaCapabilities` to verify its component contracts. `--server-project-version` remains a legacy input; conflicting capability inputs are rejected.
2. Selection precedence is explicit compatibility target, deprecated release alias, existing workbook project version, detected server project version, capability fallback, then manifest offline default.
3. Connected environments without server info use `standard` when save is available and `legacy` otherwise; disconnected authoring uses `standard` and remains disk-only.
4. Existing workbooks retain their supported persisted project version. When capabilities are available, compare the actual saved declarations with local schemas and server support, not the newer versions required by new-workbook templates. Validate again after transformations so introduced settings/components must also be supported.
   Component capability constraints apply even to explicit targets and existing workbooks. The driver rejects unsupported declarations before canonicalization, upload, or save rather than rewriting version strings. For new workbooks on a server beyond all installed profile ranges, it selects the newest supported older profile without raising its authored version. If capabilities are absent, OAC's backward-compatibility guarantee permits a profile on servers beyond that profile's known range, with a warning; within known ranges conservative baseline selection remains in effect. A newer server does not require a skill update merely to keep authoring older workbooks. An existing workbook with an unknown version still requires a newer skill. Do not remove settings or filters to bypass validation.
5. `26.05`, `26.07`, `26.09`, `26.10`, `26.11`, and `26.12` are accepted only as deprecated migration aliases.
6. Resolve contracts, templates, and schemas from `<PROFILE_ROOT>`.
7. Resolve plugin profile/runtime family from `viz-resolution-profiles.v1.json`.
8. Use `plugin-type-aliases.v1.json` only as compatibility fallback metadata.
9. Resolve runtime family/dialect from `runtime-profile-contracts.v1.json`.
10. Use the default dialect first and one fallback only for known runtime/save signatures.

## 6) Template and mapping contract

1. Template IDs are stable; internals are runtime-hardened.
2. `template-index.json` declares runtime family and required semantic checks.
3. `metadata-to-json-mapping.v1.json` defines plugin-family-specific binding:
4. Viz-lock policy is strict: requested plugin type must match final plugin type unless explicit fallback override provides fallback plugin type + reason.
4. table family -> no column edge layers.
5. autoviz chart family -> measuresList + nested MeasureView + hidden color measure + propertyAdditions.
6. combo multilayer family -> combo viewConfig `dataLayersInfo`, logicalDataModel `dataLayersInfo`, per-layer nested MeasureView models.
7. pivot/gantt/parallel/performance_tile -> family invariants and required logical/data edge bindings.
8. filter controls are profiling-driven: choose mode/operator/defaults from filter profiling output with `filter_bar` as default mode.
9. number formatting is view-level configuration, not workbook-root metadata:
10. for chart-family settings use `viewConfig.settings["viz:chart"]`.
11. use `numberFormat` for shared/default formatter and per-field override keys with existing chart key style (for example `numberFormat.<fieldLabel>`).
12. number-format payload must follow schema fields (`style`, `currency`, `useGrouping`, `minimumFractionDigits`, `maximumFractionDigits`, `useAbbreviation`, `abbreviationScale`, `negativeValuesStyle`, `currencyDisplay`).
13. number-format enum values must be save-compatible: `abbreviationScale` is `off|on|thousand|million|billion|trillion` (`on` means automatic abbreviation), and `negativeValuesStyle` is `default|accounting|red|red_accounting` (`default` means minus-sign negatives).
14. normalize unsupported aliases before validation/save: `abbreviationScale:"auto"` -> `on`, `negativeValuesStyle:"minus"` -> `default`.
15. presentation polish is deterministic and contract-driven:
14. uses `presentation-polish-contracts.v1.json` neutral_v2 theme + runtime-family style overlays + layout archetypes (`executive_dashboard`, `filter_bar`, `filter_rail`, `content_grid`, `cover`)
15. emits UX lint findings as warnings in `auto`, and fails in `strict` when severe issues are present.

## 7) Runtime invariants by family

1. `table`
2. column edge layers must be empty
3. row edge contains every requested table field in order, may mix dimensions, temporal fields, and measures, and supports 1-50 fields to match the table data-model governor
4. logical and execution row edges must contain the same ordered column IDs, and the logical column edge must be empty

5. `chart_autoviz`
6. `viewConfig.settings['obitech-autoviz/autoviz'].innerPluginType` must match plugin type
7. main data model must include `measuresList` with `type:view`, `name:MeasureView_0`
8. `nestedViews` must include embedded `MeasureView_0`
9. logical color edge must include hidden measure layer
10. nested measure property additions must include `colorMin`, `colorMax`, `color`
11. donut (`oracle.bi.tech.chart.donut`) must also include `min.<measure>` and `max.<measure>` property additions

12. `chart_combo_multilayer`
13. combo viewConfig must include `oracle.bi.tech.chart.comboMultiLayerChart.settings.dataLayersInfo`
14. logicalDataModel must include `dataLayersInfo.dataLayers` + `activeDataLayer`
15. nested `MeasureView_0` must include one data model per declared layer with non-empty measure bindings

16. `pivot`
17. requires row, column, and measures logical edge bindings
18. compact `rows`, `columns`, and `measures` arrays are ordered planning inputs; every requested entry must appear on the corresponding logical edge, mirrored execution edge, or `measuresList`

19. `gantt`
20. requires row/category binding and logical `item` edge with start/end tags (`obitech-gantt#start`, `obitech-gantt#end`)

21. `parallel_coordinates`
22. requires row binding and at least two measures on logical `col` edge

23. `performance_tile`
24. requires logical measures binding and primary measure presence

25. `map`
26. geography/category fields must bind through `logicalEdges.detail` and execution row
27. metrics must bind through map-specific measure/color/size/layer roles, never execution column because OAC renders that role as `Unused`

28. `ui_control`
29. plugin type must be mapped; no chart/table data-model assumptions are required

30. profile-required report config service nodes must exist
31. required in current profile: shape/color scheme services + project settings
32. calculations must satisfy:
33. `GLOBAL_CALC_REFERENCES_RESOLVE`
34. `GLOBAL_CALC_REFERENCE_NO_CYCLES`
35. `GLOBAL_TYPED_CALC_COLUMN_PROPERTY_MAP`
36. `GLOBAL_CALC_REFERENCE_ORDERING`
37. `GLOBAL_UNSUPPORTED_FOREIGN_FORMULA_DIALECT`
38. filter parameter bindings must resolve through `GLOBAL_FILTER_PARAMETER_BINDINGS_RESOLVE`
39. data actions must satisfy `GLOBAL_DATA_ACTIONS_SOURCE_SCHEMA`
40. criteria filter placeholders such as `None`/`All` emit warning `GLOBAL_SENTINEL_DEFAULT_FILTER_PREDICATE`
## 8) Filter-control contract

For filter-enabled templates, all are mandatory and synchronized:

1. `filterControlCollections`
2. `filterControlCollectionRef`
3. per-control `filterControlConfig`
4. per-control `filterControlSource`
5. per-control `filterControlDefaultValues`
6. criteria linkage for each filter control `columnID`
7. any `filterControlDefaultValues.*ParameterBinding` must match `parameters.settings[].name`
8. generated shared listbox parameter bindings use `parameters._version="1.0.5"` and multi-value text parameter settings
9. specific filter defaults and choices use save-compatible object values: `filterControlDefaultValues.children[]` entries are `{"text":"<value>"}` and `filterControlSource.filterControlChoices.children[]` entries are `{"value":{"text":"<value>"}}`
10. do not persist placeholder UI defaults such as `None` or `All` into `criteria.filter`; persist real query predicates only
11. when trace mode is requested, include filter decision trace in output with probe outcomes and fallback reason when any probe fails

Use `<PROFILE_ROOT>/templates/bar_with_canvas_filter_control.json` as canonical reference.

## 9) Data action contract

1. Workbook data actions are top-level `dataActions[]`; do not use `dataActions.children`.
2. Each entry must include `obitech-report/dataaction.AbstractDataAction`.
3. BI Navigation entries must use `obitech-report/dataaction.BINavigationDataAction` with target item/canvas fields and parameter mapping fields from the Oracle Analytics Cloud workbook schema.
4. URL/HTTP entries must use `obitech-report/dataaction.AbstractHTTPDataAction` with `sURL`; reject `javascript:`, `data:`, `vbscript:`, and `file:` schemes.
5. `aContextColumns` and `aAnchorToColumns` must reference criteria column IDs.

## 10) Validation Check and remediation commands

Canonical regenerate driver (recommended):

```bash
node .workbook-authoring/tools/regenerate-workbook.mjs --request <request.json> [--compatibility-target "<auto|legacy|standard|baseline|current|pv-N>"] [--server-project-version "<N>"] [--server-info-file <server-info.json>] [--target-version "<deprecated YY.MM alias>"] [--output <workbook.json>]
```

Manual runtime-validation-check commands remain available for patch/retry workflows and direct debugging:

Canonicalization validation check:

```bash
node .workbook-authoring/tools/runtime-validation-check.mjs --input <workbook.json> [--compatibility-target "<auto|legacy|standard|baseline|current|pv-N>"] [--server-project-version "<N>"] [--server-info-file <server-info.json>] [--target-version "<deprecated YY.MM alias>"] [--oac-mcp-connected "<true|false>"] --requested-plugin-type "<pluginType>" --discovery-method "<search_catalog|discover_data>" --save-available "<true|false>" --export-available "<true|false>" [--export-requested "<true|false>"] --apply-known-patches --in-place
```

Then run strict semantic validation check:

```bash
node .workbook-authoring/tools/runtime-validation-check.mjs --input <workbook.json> [--compatibility-target "<auto|legacy|standard|baseline|current|pv-N>"] [--server-project-version "<N>"] [--server-info-file <server-info.json>] [--target-version "<deprecated YY.MM alias>"] [--oac-mcp-connected "<true|false>"] --requested-plugin-type "<pluginType>" --discovery-method "<search_catalog|discover_data>" --save-available "<true|false>" --export-available "<true|false>" [--export-requested "<true|false>"]
```

Modify-mode semantic validation check:

```bash
node .workbook-authoring/tools/runtime-validation-check.mjs \
  --input <workbook.json> \
  --server-info-file <server-info.json> \
  --discovery-method "<search_catalog|discover_data>" \
  --save-available "<true|false>" \
  --export-available "<true|false>" \
  [--export-requested "<true|false>"] \
  [--version-selection-reason "<default_policy|user_requested_newer|required_newer_behavior|capability_heuristic_2607|capability_heuristic_2607_missing_fallback_latest|capability_heuristic_2605|capability_heuristic_2605_missing_fallback_latest|validation_fallback|session_sticky>"] \
  --authoring-mode "modify_existing" \
  --requested-operation "<edit_filter_values_or_operator|add_filter_bar_filter|edit_titles>" \
  --source-mode "<catalog_read|session_fast_path>" \
  --confirmation-state "confirmed" \
  --resolved-workbook-id "<targetId>"
```

If validation check returns `INPUT_ARTIFACT_NOT_READY`, the workbook file was missing/empty/incomplete at read time. Finish generation first, then rerun validation check.
Runtime validation check capability inputs are mandatory and must come from runtime tool detection. If `--discovery-method`, `--save-available`, or `--export-available` is missing, validation check fails fast with `MISSING_EXECUTION_CAPABILITY_INPUT`.
Pass `--export-requested true` only when user intent explicitly asks for export; omit it for default fast path (`exportRequested=false`).
Pass `--compatibility-target` only for explicit user intent. Pass the complete `oracle_analytics-get_server_info` response through `--server-info-file`; retain `--server-project-version` only for legacy version-only input. Pass `--oac-mcp-connected true` when OAC MCP tools are actually available; `discoveryMethod` alone does not establish connectivity. Connected fallback requires an explicit discovery-unavailability reason, not omitted server inputs.
Validation Check enforces schema-acceptance checks and strips known-safe internal trace payload keys (`oracle.bi.tech.workbookAuthoringTrace`) before save attempts.

Known-error patch prep (after save/runtime error text):

```bash
node .workbook-authoring/tools/runtime-validation-check.mjs \
  --input <workbook.json> \
  --server-info-file <server-info.json> \
  --discovery-method "<search_catalog|discover_data>" \
  --save-available "<true|false>" \
  --export-available "<true|false>" \
  [--export-requested "<true|false>"] \
  [--version-selection-reason "<default_policy|user_requested_newer|required_newer_behavior|capability_heuristic_2607|capability_heuristic_2607_missing_fallback_latest|capability_heuristic_2605|capability_heuristic_2605_missing_fallback_latest|validation_fallback|session_sticky>"] \
  --runtime-error "<error text>" \
  --apply-known-patches \
  --in-place
```

Retry policy:
1. one deterministic patch set
2. one retry
3. if still failing, return contract-gap diagnostics
4. include resolution trace when trace mode is requested:
5. `requestedPluginType`, `resolvedFamily`, `scaffoldTemplate`, `finalPluginType`, `fallbackUsed`, `reason`

## 10) Save + preview policy

1. Save and validate local JSON first. MCP save remains authoritative for backend validation. Workbook `name` / `description` belong in save-layer metadata, not the workbook JSON root.
2. Preserve save intent: update/change uses `id`; explicit new/copy uses `parentId` + `name`. Never silently create duplicates when an update target is unresolved or a prior save outcome is unknown.
3. Prefer framework upload for every workbook when `oracle_analytics-authorize_file_upload` is available, `oracle_analytics-save_catalog_content` declares the optional `contentFile` input, and the client can read file bytes and perform HTTP PUT. Keep inline `content` supported for older or upload-unavailable environments.
4. Write `{ "json": <validated-workbook-json>, "blobs": [...] }` to a UTF-8 JSON file (`blobs` optional). Upload the content object, not the complete tool arguments. Measure exact bytes and authorize with the save tool name, `inputName: "contentFile"`, filename, `mimeType: "application/json"`, and exact `size`.
5. PUT raw bytes to the returned URL with all required headers before ticket expiry. Do not print or log the header token. Confirm upload success, then call save with the original opaque URI in `contentFile`, omitting `content`. Keep metadata, destination, and approval arguments in the save call.
6. The file input accepts at most 100 MiB, further limited by operator settings. Existing backend limits still apply: workbook and blob request bodies must each be under 150 MiB; the workbook-size limit defaults to 100 MiB. Upload acceptance does not guarantee save acceptance.
7. Select transport once. A selected upload failure must not trigger inline/marker fallback or reduced workbook fidelity. Interrupted or expired transfers need a new authorization. Uploaded-file URIs, local filesystem paths, and the capability-gated AIR argument-file marker are different contracts.
8. If upload is unavailable or the save schema lacks `contentFile`, retain the client's supported alternatives: an exact managed argument-file marker only when explicitly advertised by active developer instructions, otherwise canonical inline content. Preserve the inline-only compaction/interop fallback and at most one retry. Any existing reduced-size retry remains confined to inline transport; managed-marker rejection remains terminal.
9. A save attempt consumes the uploaded file on success or failure. Approval deferral retains it until expiry (`fileCleanup: "retained"`). After consumption, an eligible deterministic retry requires another authorization and upload; reconcile unknown or partial mutation outcomes first.
10. `fileCleanup` reports `deleted`, `queued`, or `unconfirmed` for attempted disposal. Preserve the save outcome independently; never repeat a successful save just to retry cleanup.
11. On save success, publish the saved identifier and `viewUrl` immediately. Export only when explicitly requested and available. If save is unavailable, return the local artifact and disk-only outcome; do not attempt remote replacement.
12. For modify mode, remote save is replace-by-id only; keep modify traces outside workbook JSON and show them only in trace mode.

## 11) Server sample fallback policy

1. Default path must not inspect server workbook JSON.
2. Fallback is allowed only when validation check passes and one remediation retry still fails runtime/visual acceptance.
3. When fallback is used, report the missing contract rule(s).

## 12) Output modes

Default user output is concise and should prioritize save outcome and `viewUrl`.

By default, return:
1. local JSON path (if produced)
2. saved workbook id/path (or deterministic disk-only outcome)
3. `viewUrl` on save success
4. selected `compatibilityTarget`
5. export summary only when explicitly requested and completed
6. advisory `dvIntelligenceSummary` (`overallScore`, `audienceLevel`, `dimensionScores`, `recommendations`, `evidenceCoverage`, `versionProfile`)

Trace mode is opt-in and should be enabled only when user explicitly requests `trace`, `debug`, `diagnostics`, or sets `traceRequested=true`.

When trace mode is enabled, emit:
1. `compatibilityTarget`
2. `executionMode`
3. `reasonForVersionSelection`
4. `capabilitySource`
5. `saveToolDetected`
6. `exportToolDetected`
7. `discoveryMethod`
8. `saveAvailable`
9. `exportAvailable`
10. `exportRequested`
1. `requestedSaveIntent`
2. `resolvedSaveMode` (`replace_existing` or `create_new`)
3. `resolvedWorkbookTarget` (`id`/path/name)
4. `createBlockedByCollision`
5. `reason`
6. in modify mode: `modifyTrace.requestedOperation`, `modifyTrace.resolvedWorkbookTarget`, `modifyTrace.sourceMode`, `modifyTrace.confirmationState`, `modifyTrace.mutationsApplied`, `modifyTrace.pathsChanged`, `modifyTrace.fallbackUsed`, `modifyTrace.fallbackReason` (output trace only; not workbook payload)
7. never expose internal channel keys or internal `projectVersion` IDs in user-facing output
8. include `dvIntelligenceTrace` only in trace mode
