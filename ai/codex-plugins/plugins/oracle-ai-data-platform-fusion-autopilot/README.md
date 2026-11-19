# Oracle AI Data Platform Fusion Autopilot for Codex

A Codex plugin for building and operating Oracle Fusion ERP, HCM, and SCM
analytics on Oracle AI Data Platform (AIDP). It combines 16 reusable Skills, a
guarded Python CLI, content-pack resources, customer scaffolds, and an optional
Oracle Analytics Cloud (OAC) MCP server definition.

This package was migrated from the source assistant package into Codex plugin
structure. Repository-only development history, local credentials, caches,
virtual environments, generated evidence, and source-platform configuration are
not included.

## Requirements

- Codex with plugin support
- Python 3.10 or newer
- Network access on first CLI use to install the pinned Python dependencies into
  plugin data
- Node.js and the OAC MCP connector when using OAC MCP workflows
- Access to the relevant Oracle Fusion, AIDP, OCI, and OAC environments

Never commit credentials. Start from `.env.example` and keep secrets in the
supported local credential or vault flow.

## Install

When this plugin is published in a configured Codex marketplace, install it
with:

```bash
codex plugin add oracle-ai-data-platform-fusion-autopilot@<marketplace-name>
```

For a non-default local marketplace, first register the directory containing
its `.agents/plugins/marketplace.json`:

```bash
codex plugin marketplace add /path/to/marketplace-root
codex plugin add oracle-ai-data-platform-fusion-autopilot@<marketplace-name>
```

Start a new Codex thread after installation so the Skills and MCP server are
loaded.

## Configure OAC MCP

The plugin-level `.mcp.json` launches the connector configured by
`OAC_MCP_CONNECT_PATH`:

```bash
export OAC_MCP_CONNECT_PATH=/absolute/path/to/oac-mcp-connect.js
```

The guided workflow can also stage and configure the connector:

```bash
"/path/to/plugin/bin/aidp-fusion-autopilot" dashboard mcp-setup \
  --connector-js /absolute/path/to/oac-mcp-connect.js
```

Restart or reconnect Codex after changing MCP configuration. Keep the OAC user
least-privileged because connector capabilities are governed by that user's
grants.

## Use

For an end-to-end goal, invoke:

```text
$aidp-fusion-autopilot Build a CFO dashboard for supplier spend, AP aging, and GL balance from this Fusion tenant.
```

For the overview and manual workflow, invoke `$aidp-fusion-pipeline`. Focused
Skills are available for configuration, bootstrap, seed, incremental refresh,
status, error triage, drift diagnosis, medallion or mart authoring, OAC dataset
planning/setup, and workbook authoring.

The CLI ships with the plugin. Resolve `<plugin-root>` as this README's
directory, then invoke:

```bash
"<plugin-root>/bin/aidp-fusion-autopilot" --help
"<plugin-root>/bin/aidp-fusion-autopilot" init
"<plugin-root>/bin/aidp-fusion-autopilot" validate
```

On Windows, use `<plugin-root>\bin\aidp-fusion-autopilot.cmd`. Do not assume the
plugin's `bin/` directory is automatically added to `PATH`.

## Package Layout

```text
.codex-plugin/plugin.json   Codex plugin manifest
.mcp.json                   Optional OAC MCP server definition
skills/                     Sixteen Codex Skill entrypoints and references
bin/                        Cross-platform CLI wrappers
scripts/                    Python runtime and pinned dependencies
examples/                   Customer bundle scaffolds
notebooks/                  AIDP orchestration notebook
oac/                        OAC templates and semantic-model resources
overlays/                   Additive content-pack overlays
profiles/                   Tenant-profile templates
```

## License

MIT. See `LICENSE`.
