# Workbook Authoring Skill: End User Guide

This guide is for users who want to create or update Oracle Analytics workbooks through an AI coding agent.

## What this skill helps you do

1. Create new workbooks from natural-language analysis requests.
2. Generate multi-canvas and multi-visualization workbooks.
3. Apply common workbook updates, including filter changes and title updates.
4. Apply first-pass presentation polish for `compose_ootb` requests (layout normalization, title normalization, UX lint warnings).
5. Save generated workbooks to OAC and return a workbook link when save is available.
6. Optionally export previews when requested.

## Prerequisites

1. Your AI client must be configured with an Oracle Analytics Cloud MCP connection.
2. OAC MCP setup guide:
3. <https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/add-oracle-analytics-cloud-mcp-server-your-ai-client-preview.html>

## Installation model

1. Install one workbook-authoring skill bundle (`workbook-authoring-skills-<buildVersion>.zip`).
2. The bundle includes shared skill files plus all supported target-version payload folders.
3. Installed versions are listed in `.workbook-authoring/version-bundles.json`.
4. For local `npx skills` installs, unzip `workbook-authoring-skills-installable-<buildVersion>.zip`, then run:
5. `npx skills add ./workbook-authoring`
6. For future plugin-publish workflows, use `workbook-authoring-plugin-<buildVersion>.zip`.
7. No manual asset copy is required; installable/plugin packages are self-contained.

## Choosing OAC target version

1. If only one target version is installed, the skill auto-selects it.
2. If multiple versions are installed and you specify `targetVersion`, that version is used.
3. If multiple versions are installed and `targetVersion` is omitted, runtime auto-selects deterministically and returns selected `targetVersion` in output.

## Recommended way to ask your agent

Use clear business intent plus expected layout. Include target version when multiple versions are installed.

Example requests:
1. "Create a sales performance workbook for FY2025 with 2 canvases: executive summary and regional deep dive."
2. "Target version 26.05. Build a workbook with bar, line, and table visuals for revenue, margin, and units by month."
3. "Update the existing workbook and rename canvas titles to Executive Summary and Trend Analysis."
4. "Create a new copy in /Shared/DV and return the workbook URL."

## Best practices

1. The agent may infer canvas names, but provide them explicitly for guaranteed first-pass labeling.
2. State whether you want a new workbook or an update to an existing one.
3. Include metric formatting requirements early (currency, decimals, abbreviations, negatives).
4. Ask for trace/diagnostics only when troubleshooting.
5. If you want to control presentation polish, ask explicitly:
6. default is `presentationPolish.mode=auto` for both `compose_ootb` and `passthrough_bound`
7. use `presentationPolish.mode=off` to disable polish
8. use `presentationPolish.mode=strict` to fail on severe UX issues before save
9. ask for polish telemetry (`effectiveChangeCount`, `layoutChangeCount`, `styleChangeCount`, `noOpReasons`) when validating first-pass presentation quality

## What to expect from output

1. A generated workbook artifact.
2. Save result (when save capability is available).
3. Workbook link (`viewUrl`) on successful save.
4. Optional export artifact if explicitly requested.

## Known boundaries

1. The skill is optimized for deterministic workbook authoring flows, not arbitrary manual JSON editing.
2. Some visualization-specific runtime nuances may still require follow-up refinement in rare cases.
3. Advanced custom structures are supported best when clearly specified in your request.

## If something fails

1. Re-run with explicit target version (in multi-version installs).
2. If workbook JSON is large enough that Codex MCP argument limits are likely, ask the agent to minify the same JSON object before saving through the MCP save tool.
3. If save fails due to tool-call payload constraints, ask the agent to retry with payload compaction (minified JSON) while preserving semantic equivalence.
4. If truncation/argument-size limits persist after compaction, ask for a lean retry first with `numberFormatting.policy=none` and `presentationPolish.mode=off`, then retry minified save payload.
5. Do not use file path / `jsonPath` references for workbook save payloads; `save_catalog_content` requires inline JSON object or stringified JSON content.
6. Ask the agent to return diagnostics and contract-gap summary.
7. Confirm OAC MCP save/export capabilities are available in your environment.
8. Retry with tighter scope (single subject area, explicit measures/dimensions), then expand.
9. If strict presentation polish was enabled, ask for reported UX lint findings and retry with adjusted layout hints.

## Report an issue with a feedback package

1. Ask the agent to prepare a feedback package when generation/check/save/export/UI runtime fails.
2. Supported package modes:
3. `full` (default): includes raw artifacts when available for fastest internal debugging.
4. `sanitized`: includes redacted summaries and masks sensitive fields; raw workbook JSON is omitted by default.
5. Package contract:
6. folder name: `feedback-<YYYYMMDD-HHMMSS>-<short_slug>`
7. required files: `ISSUE_REPORT.md`, `feedback_manifest.json`, `environment_context.json`
8. The agent should always create the package locally first and then ask whether you want to share it.
9. Use `sanitized` for external sharing; `full` is acceptable for internal skill maintainer debugging.
