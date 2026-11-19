# Status - `oracle-ai-data-platform-fusion-autopilot`

Last reviewed: 2026-06-15.

This is the current status page for the content-pack plugin. It is not a phase
report. Historical v1/v2 transition notes are local working notes under the
ignored `dev/` directory and are not published user guidance.

For the operator workflow, use [workflow.md](workflow.md). For fresh setup, use
[docs/project_setup.md](docs/project_setup.md). For active work, use
[BACKLOG.md](BACKLOG.md).

## Current State

| Area | Status |
|---|---|
| Package | Alpha, version `0.1.0a0` in `pyproject.toml`. |
| Execution model | Single content-pack path. New bronze, silver, and gold work is YAML/SQL content-pack work, not legacy Python `dim_*.py` or `transforms/gold/*.py` modules. |
| Starter pack | `fusion-finance-starter` ships 11 bronze datasets, 3 silver dimensions, 3 gold marts, and dashboard descriptors. |
| Customer starter | `aidp-fusion-autopilot init` writes only `bundle.yaml` and `aidp.config.yaml`. It does not copy bronze/silver/gold node YAML into the customer project. |
| Customer extension | Overlays under `overlays/<name>/`, wired with `aidp-fusion-autopilot use-pack`. Use `--no-align` for narrow bundles or one-mart overrides. |
| OAC path | MCP-native workbook authoring is the preferred path. Legacy `.bar` snapshot install still ships for deployments that need it. |
| Manual OAC boundary | Users still create the OAC AIDP connection and OAC dataset manually. The plugin can advise and generate connection JSON, but OAC UI remains the reliable creation path. |
| Conversational entry point | `aidp-fusion-autopilot` drives the A-to-Z workflow and routes to setup, seed, advisor, mart-author, medallion-author, status, and workbook-authoring skills. |

## Current Workflow

The expected customer journey is:

1. Install the CLI from this checkout.
2. Create a customer bundle with `aidp-fusion-autopilot init`.
3. Resolve AIDP workspace and cluster coordinates with `init-config`, or use
   `/aidp-fusion-config` when the user does not know the OCIDs/keys.
4. Put the Fusion BICC password in the AIDP credential store. The default
   credential name is `fusion_bicc_password` and the default key is `password`.
5. Set up operator OAC MCP early with `dashboard mcp-setup` or
   `dashboard mcp-token`, then restart or reconnect Claude Code.
6. Run `aidp-fusion-autopilot validate`.
7. Run `aidp-fusion-autopilot bootstrap --check-iam` to probe prerequisites and
   pin tenant variation into `profiles/<profile>.yaml`.
8. Preview and run seed with `aidp-fusion-autopilot run --mode seed --dry-run`
   followed by `aidp-fusion-autopilot run --mode seed`.
9. Use `/oac-dataset-advisor` against the live AIDP catalog.
10. If a needed mart is missing, use `/mart-author` for a new gold overlay or
    SQL override.
11. Create the OAC AIDP connection and dataset manually in OAC.
12. Resume autopilot or use `/workbook-authoring` to save the workbook through
    OAC MCP.

## Current Docs

| Need | Source |
|---|---|
| Full operator workflow | [workflow.md](workflow.md) |
| Fresh checkout and customer setup | [docs/project_setup.md](docs/project_setup.md) |
| Content-pack execution details | [docs/content_pack_execution.md](docs/content_pack_execution.md) |
| New mart or mart override | [docs/mart_overlay_authoring.md](docs/mart_overlay_authoring.md) |
| OAC MCP setup | [docs/oac_mcp_setup.md](docs/oac_mcp_setup.md) |
| OAC REST setup and legacy `.bar` flow | [docs/oac_rest_api_setup.md](docs/oac_rest_api_setup.md) |
| Workbook binding and save flow | [docs/oac_workbook_authoring_e2e.md](docs/oac_workbook_authoring_e2e.md) |
| `AIDPF-*` errors | [docs/aidpf-error-codes.md](docs/aidpf-error-codes.md) |
| Current backlog | [BACKLOG.md](BACKLOG.md) |

## Architecture Authority

- [docs/adr/0021-pack-as-registry.md](docs/adr/0021-pack-as-registry.md)
  records the decision that content packs are the silver/gold registry.
- [workflow.md](workflow.md) records the current operator state machine.
- [docs/mart_overlay_authoring.md](docs/mart_overlay_authoring.md) records the
  supported extension model.

Do not use old phase reports to decide current behavior unless the code and
current docs confirm the same behavior.

## Known Boundaries

- OAC AIDP connection creation is manual because the public OAC REST validator
  does not reliably accept first-time AIDP `idljdbc` connection creation.
- OAC dataset creation is manual because the OAC MCP connector can inspect,
  query, and save catalog content, but does not expose a create-dataset tool.
- Seed is intentionally fail-closed. The laptop CLI cannot prove every physical
  target table is empty, so first materialization requires explicit operator
  confirmation.
- Content-pack YAML proves what can be built. It does not prove a live table or
  OAC dataset already exists. Advisor/status skills must use live catalog
  evidence.
- New customer medallion work belongs in overlays, not in the shipped starter
  pack.

## Current Open Work

Use [BACKLOG.md](BACKLOG.md) as the active task source. The highest-priority
documentation work is:

- Finish top-level historical/current cleanup for `CLAUDE.md`.
- Keep setup, workflow, and overlay docs aligned with `init`, `use-pack`, and
  the manual OAC dataset boundary.
- Keep every referenced `AIDPF-*` code documented in
  [docs/aidpf-error-codes.md](docs/aidpf-error-codes.md).
- Keep local Markdown links valid through `make docs-check`.
- Decide whether the currently untracked `docs/features/` and
  `tests/live/dispatch_bicc_smoke.py` paths should be tracked, moved, or
  deleted as scratch output.
