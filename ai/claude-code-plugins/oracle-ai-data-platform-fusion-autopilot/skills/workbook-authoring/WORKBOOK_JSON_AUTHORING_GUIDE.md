# Workbook JSON Authoring Guide (v1.2 Runtime-Valid)

This guide defines deterministic workbook authoring that targets runtime-valid plugin contracts, not just schema-valid JSON.

## 1) Required source artifacts

1. `<TARGET_VERSION>/model/metadata-to-json-mapping.v1.json`
2. `<TARGET_VERSION>/model/filter-profiling-contracts.v1.json`
3. `<TARGET_VERSION>/model/calculation-contracts.v1.json`
4. `<TARGET_VERSION>/model/version-field-catalog.json`
5. `<TARGET_VERSION>/model/runtime-profile-contracts.v1.json`
6. `<TARGET_VERSION>/model/semantic-validation-rules.v1.json`
7. `<TARGET_VERSION>/model/plugin-type-aliases.v1.json`
8. `<TARGET_VERSION>/model/viz-runtime-catalog.v1.json`
9. `<TARGET_VERSION>/model/viz-resolution-profiles.v1.json`
10. `<TARGET_VERSION>/model/presentation-polish-contracts.v1.json`
11. `<TARGET_VERSION>/model/edit-operation-contracts.v1.json`
12. `<TARGET_VERSION>/model/support-window.v1.json`
12. `<TARGET_VERSION>/templates/template-index.json`
13. `tools/runtime-validation-check.mjs`
14. `tools/modify-workbook.mjs`
15. `tools/regenerate-workbook.mjs`
16. `tools/validate-requirements-trace.mjs`
17. `<TARGET_VERSION>/model/regenerate-workbook-contract.v1.json`
18. `<TARGET_VERSION>/model/regenerate-workbook-adapter-contract.v1.json`

## 2) Canonical generation order

1. Parse intent.
2. Route to authoring mode:
3. `generate_fresh` (primary default), or
4. `modify_existing` (limited `edit_filter_values_or_operator` / `add_filter_bar_filter` / `edit_titles`).
5. Resolve save target mode (`replace_existing` vs `create_new`) before generation/mutation.
6. For `modify_existing`, resolve exact workbook target (`id`) and require explicit confirmation.
7. Metadata discovery (use `find_matching_datasources` first when available, then `search_catalog` for authoritative catalog resolution, use `discover_data` only as compatibility fallback when newer discovery tools are unavailable, then two-phase `describe_data`).
8. Filter profiling using `oracle_analytics_execute_logical_sql` with deterministic probes from `filter-profiling-contracts.v1.json`.
9. Requirements gate for `compose_ootb`: build requirement artifact, require explicit approval, and persist as `analysisRequirements`.
10. Resolve plugin profile (`pluginType -> runtimeContractFamily + canonicalScaffoldTemplateId + finalPluginType`).
11. Profile handshake (support-window target version + internal schema target ID + runtime dialect).
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
node .workbook-authoring/tools/regenerate-workbook.mjs --request <request.json> [--target-version "<YY.MM>"] [--detected-target-version "<YY.MM>"] [--output <workbook.json>]
```

The request payload must satisfy `<TARGET_VERSION>/model/regenerate-workbook-contract.v1.json`, and `adapterPayload` must satisfy `<TARGET_VERSION>/model/regenerate-workbook-adapter-contract.v1.json`.
When request includes `workbook.name` / `workbook.description`, treat them as save metadata only. They must not be persisted at workbook JSON root inside `content.json`; consume driver response `saveMetadata` for save-layer handoff.
Recommend providing `analysisShape.canvases[].name` during initial generation when canvas labels are known. This field is optional and omitted names do not block generation.
For `compose_ootb`, `analysisRequirements` is required and must be explicitly approved. `analysisShape.canvases[].views[]` must include `purpose`, `grain`, `bindings`, `labels`, `filters`, `calculations`, and `sort` (optional `interactions`).
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
`generationStrategy` controls hybrid regenerate behavior:
1. `auto` (default): use passthrough when bound workbook input exists, else compose OOTB.
2. `compose_ootb`: deterministic OOTB topology composition from `analysisShape` for supported 80/20 multi-canvas/multi-viz cases.
3. `passthrough_bound`: preserve bound workbook topology as advanced/custom escape hatch and enforce only base contract + validation check.
`presentationPolish` controls additive presentation normalization:
1. `mode`: `auto | off | strict`
2. `layoutTemplateHints`: optional `defaultArchetype`, `byCanvasID`, `byCanvasIndex`
3. `titlePolicy`: optional `question_oriented | preserve_input`
4. defaults: `compose_ootb -> auto`, non-`compose_ootb -> auto` unless explicitly disabled
5. `strict` mode fails generation on severe UX lint findings before save.
6. `presentationPolishSummary` must include effect telemetry fields: `effectiveChangeCount`, `layoutChangeCount`, `styleChangeCount`, and `noOpReasons`.
`visualizationIntelligence` controls advisory-only DV scoring:
1. `mode`: `auto | off` (default `auto`)
2. `audienceProfile`: optional object (for example `role`, `targetLevel`) used to tune recommendation wording.
3. scoring failures are non-blocking; emit `dvIntelligenceSummary.status=scoring_unavailable` and continue.
Regenerate response telemetry must include `evidenceLevel`, `requirementsTraceSummary`, `filterPlanningSummary`, `componentGraphSummary`, `fallbackUsageSummary`, and `dvIntelligenceSummary`.
Requirements-trace severity model:
1. blocking mismatches fail compose (`REQUIREMENTS_TRACE_VALIDATION_FAILED`).
2. warnings are advisory-only and returned in `requirementsTraceSummary.warnings`.

## 2.1) Modify-v1 operation contract

1. Source of truth: `<TARGET_VERSION>/model/edit-operation-contracts.v1.json`.
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

1. Calculation source of truth is `<TARGET_VERSION>/model/calculation-contracts.v1.json`.
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

1. Profiling source of truth is `<TARGET_VERSION>/model/filter-profiling-contracts.v1.json`.
2. `oracle_analytics_execute_logical_sql` is required for profiling probes.
3. Probe classes:
4. dimensions: top values + cardinality estimate.
5. measures: min/max + optional bounded distribution sample.
6. temporal: min/max + granularity hint.
7. Guardrails are mandatory: deterministic ordering, explicit fetch limits, bounded row/time thresholds, bounded retries.
8. Probe-level failures do not stop generation; they trigger conservative fallback with explicit trace reason.
9. Default filter policy remains filter-bar-first with moderate auto-filter selection.
10. High-cardinality candidates are de-prioritized unless explicitly requested.
11. Trace-mode output fields (include only when user explicitly requests `trace`, `debug`, `diagnostics`, or `traceRequested=true`):
12. `selectedFilterMode`, `queryIntents`, `probeResults`, `derivedDecisions`, `fallbackUsed`, `fallbackReason`.

## 5) Profile handshake

1. Resolve target version from installed `.workbook-authoring/<YY.MM>/` bundles.
2. If `targetVersion` is provided, it must match an installed bundle.
3. If exactly one bundle is installed and `targetVersion` is omitted, runtime auto-selects it.
4. If multiple bundles are installed and `targetVersion` is omitted, runtime selects latest when discovery is `search_catalog` and save is available; otherwise runtime prefers `26.05` when installed, else latest.
5. If a prior save validation payload contains version-mismatch signatures, pass `detectedTargetVersion` (or `--detected-target-version`) to keep retries sticky to the last accepted target.
6. Always report selected `targetVersion` in user-visible output.
7. Resolve selected-version contracts from `<TARGET_VERSION>/model/*` and templates/schemas from `<TARGET_VERSION>/...`.
8. Resolve the internal schema target ID from the selected support-window target.
9. Resolve plugin profile/runtime family from `viz-resolution-profiles.v1.json`.
10. Use `plugin-type-aliases.v1.json` only as compatibility fallback metadata.
11. Resolve runtime family/dialect from `runtime-profile-contracts.v1.json`.
12. Use default dialect first.
13. Allow one fallback dialect only when runtime/save errors match known signatures.

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
3. row edge must include dimension + measure
4. logical column edge must be empty

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

18. `gantt`
19. requires row/category binding and logical `item` edge with start/end tags (`obitech-gantt#start`, `obitech-gantt#end`)

20. `parallel_coordinates`
21. requires row binding and at least two measures on logical `col` edge

22. `performance_tile`
23. requires logical measures binding and primary measure presence

24. `map`
25. geography/category fields must bind through `logicalEdges.detail` and execution row
26. metrics must bind through map-specific measure/color/size/layer roles, never execution column because OAC renders that role as `Unused`

27. `ui_control`
28. plugin type must be mapped; no chart/table data-model assumptions are required

29. profile-required report config service nodes must exist
30. required in current profile: shape/color scheme services + project settings
31. calculations must satisfy:
32. `GLOBAL_CALC_REFERENCES_RESOLVE`
33. `GLOBAL_CALC_REFERENCE_NO_CYCLES`
34. `GLOBAL_TYPED_CALC_COLUMN_PROPERTY_MAP`
35. `GLOBAL_CALC_REFERENCE_ORDERING`
36. `GLOBAL_UNSUPPORTED_FOREIGN_FORMULA_DIALECT`
37. filter parameter bindings must resolve through `GLOBAL_FILTER_PARAMETER_BINDINGS_RESOLVE`
38. data actions must satisfy `GLOBAL_DATA_ACTIONS_SOURCE_SCHEMA`
39. criteria filter placeholders such as `None`/`All` emit warning `GLOBAL_SENTINEL_DEFAULT_FILTER_PREDICATE`
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
9. do not persist placeholder UI defaults such as `None` or `All` into `criteria.filter`; persist real query predicates only
10. when trace mode is requested, include filter decision trace in output with probe outcomes and fallback reason when any probe fails

Use `<TARGET_VERSION>/templates/bar_with_canvas_filter_control.json` as canonical reference.

## 9) Data action contract

1. Workbook data actions are top-level `dataActions[]`; do not use `dataActions.children`.
2. Each entry must include `obitech-report/dataaction.AbstractDataAction`.
3. BI Navigation entries must use `obitech-report/dataaction.BINavigationDataAction` with target item/canvas fields and parameter mapping fields from BI Tech source schema.
4. URL/HTTP entries must use `obitech-report/dataaction.AbstractHTTPDataAction` with `sURL`.
5. `aContextColumns` and `aAnchorToColumns` must reference criteria column IDs.

## 10) Validation Check and remediation commands

Canonical regenerate driver (recommended):

```bash
node .workbook-authoring/tools/regenerate-workbook.mjs --request <request.json> [--target-version "<YY.MM>"] [--detected-target-version "<YY.MM>"] [--output <workbook.json>]
```

Manual runtime-validation-check commands remain available for patch/retry workflows and direct debugging:

Canonicalization validation check:

```bash
node .workbook-authoring/tools/runtime-validation-check.mjs --input <workbook.json> [--target-version "<YY.MM>"] [--detected-target-version "<YY.MM>"] --requested-plugin-type "<pluginType>" --discovery-method "<search_catalog|discover_data>" --save-available "<true|false>" --export-available "<true|false>" [--export-requested "<true|false>"] [--version-selection-reason "<default_policy|user_requested_newer|required_newer_behavior|capability_heuristic_2607|capability_heuristic_2607_missing_fallback_latest|capability_heuristic_2605|capability_heuristic_2605_missing_fallback_latest|validation_fallback|session_sticky>"] --apply-known-patches --in-place
```

Then run strict semantic validation check:

```bash
node .workbook-authoring/tools/runtime-validation-check.mjs --input <workbook.json> [--target-version "<YY.MM>"] [--detected-target-version "<YY.MM>"] --requested-plugin-type "<pluginType>" --discovery-method "<search_catalog|discover_data>" --save-available "<true|false>" --export-available "<true|false>" [--export-requested "<true|false>"] [--version-selection-reason "<default_policy|user_requested_newer|required_newer_behavior|capability_heuristic_2607|capability_heuristic_2607_missing_fallback_latest|capability_heuristic_2605|capability_heuristic_2605_missing_fallback_latest|validation_fallback|session_sticky>"]
```

Modify-mode semantic validation check:

```bash
node .workbook-authoring/tools/runtime-validation-check.mjs \
  --input <workbook.json> \
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
Optional `--version-selection-reason` can be passed for explicit target-intent tracing; if omitted, validation check emits a deterministic derived fallback reason. Optionally pass `--detected-target-version` when retrying after a prior save validation version-mismatch signal.
Validation Check enforces schema-acceptance checks and strips known-safe internal trace payload keys (`oracle.bi.tech.workbookAuthoringTrace`) before save attempts.

Known-error patch prep (after save/runtime error text):

```bash
node .workbook-authoring/tools/runtime-validation-check.mjs \
  --input <workbook.json> \
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

1. Save local JSON first.
2. MCP save is authoritative for validation.
3. Workbook `name` / `description` are save-layer metadata. Do not store them at workbook JSON root in `content.json`; use regenerate response `saveMetadata` (or equivalent orchestration metadata fields) for save calls.
4. Save mode defaults:
5. update/change intent -> `replace_existing` using `oracle_analytics_save_catalog_content({ id, ... })`.
6. explicit new/copy intent -> `create_new` using `oracle_analytics_save_catalog_content({ parentId, name, ... })`.
7. Never silently create duplicates when update intent is unresolved/ambiguous; fail fast with guidance.
8. If save tool is unavailable, skip remote save and return deterministic disk-only outcome.
9. In no-save environments, modify intent falls back to regenerate-only output.
10. On save success, publish `viewUrl` immediately.
11. Export preview is asynchronous and follows only when user explicitly requests export and export tool is available.
12. If workbook JSON is already large enough that Codex MCP argument truncation/transport limits are likely, minify the same object before the first MCP save call and verify canonical deep equivalence.
13. If save payload is still truncated by tool-call size limits, run one reduced-size retry with `numberFormatting.policy=none` and `presentationPolish.mode=off`, then retry with minified JSON while preserving semantic equivalence.
14. For modify mode, save is replace-by-id only; modify trace must exist in tool/validation check output and is included in user output only in trace mode.

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
4. selected `targetVersion` (always, including auto-selected single-bundle runs)
5. export summary only when explicitly requested and completed
6. advisory `dvIntelligenceSummary` (`overallScore`, `audienceLevel`, `dimensionScores`, `recommendations`, `evidenceCoverage`, `versionProfile`)

Trace mode is opt-in and should be enabled only when user explicitly requests `trace`, `debug`, `diagnostics`, or sets `traceRequested=true`.

When trace mode is enabled, emit:
1. `targetVersion`
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
