# Documentation Index

Start here if you are trying to use or operate the plugin:

| Need | Document |
|---|---|
| Fresh checkout or customer bundle setup | [project_setup.md](project_setup.md) |
| Copyable sample bundles and overlays | [../examples/README.md](../examples/README.md) |
| Add or customize marts with overlays | [mart_overlay_authoring.md](mart_overlay_authoring.md) |
| End-to-end operator workflow | [workflow.md](../workflow.md) |
| AIDPF code meanings and recovery hints | [aidpf-error-codes.md](aidpf-error-codes.md) |
| OAC MCP setup for operators and end users | [oac_mcp_setup.md](oac_mcp_setup.md) |
| Workbook generation, binding, and save flow | [oac_workbook_authoring_e2e.md](oac_workbook_authoring_e2e.md) |
| Content-pack runner behavior | [content_pack_execution.md](content_pack_execution.md) |
| Diagnostic artifact shapes | [diagnostic-artifact-contract.md](diagnostic-artifact-contract.md) |

## Current OAC Path

The preferred dashboard path is MCP-native:

```text
aidp-fusion-autopilot
  -> OAC MCP setup
  -> bootstrap and seed AIDP gold
  -> oac-dataset-advisor
  -> oac-dataset-setup for manual OAC AIDP connection and dataset verification
  -> workbook-authoring via OAC MCP
```

The AIDP connection and OAC dataset are manual because OAC's public REST
validator does not reliably accept first-time AIDP `idljdbc` connection
creation, and OAC MCP does not expose a create-dataset tool.

## Legacy And Deep-Dive Docs

| Topic | Document |
|---|---|
| Legacy `.bar` snapshot install over OAC REST | [oac_rest_api_setup.md](oac_rest_api_setup.md) |
| REST dispatch setup for AIDP jobs | [rest_dispatch_setup.md](rest_dispatch_setup.md) |
| Medallion-author skill notes | [v2-medallion-author-skill.md](v2-medallion-author-skill.md) |
| Variation catalog and bootstrap details | [v2-phase-3-variation-catalog.md](v2-phase-3-variation-catalog.md) |
| Architecture decisions | [adr/](adr/) |
| Historical phase reports | `v2-phase-*.md` |
| Feature plans and notes | [features/](features/) |

## Validation

Run local documentation validation with:

```bash
make docs-check
```

This checks that user-facing relative Markdown links resolve on disk and that
every `AIDPF-*` code referenced in the repository appears in
[aidpf-error-codes.md](aidpf-error-codes.md).
