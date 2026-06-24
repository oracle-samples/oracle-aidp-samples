# Oracle AI Data Platform - Codex CLI Plugins

A marketplace of OpenAI Codex CLI plugins for Oracle AI Data Platform (AIDP).

## Marketplace

- **Name:** `oracle-aidp-codex`
- **Manifest:** [`.agents/plugins/marketplace.json`](./.agents/plugins/marketplace.json)

## Plugins

| Plugin | Version | Status | Purpose |
|---|---|---|---|
| [`oracle-ai-data-platform-workbench-databricks-migrator`](./plugins/oracle-ai-data-platform-workbench-databricks-migrator/) | 0.1.0 | Initial release | Plan and execute automated Databricks to AIDP migrations from Codex. |
| [`oracle-ai-data-platform-workbench-engineer-agent`](./plugins/oracle-ai-data-platform-workbench-engineer-agent/) | 0.1.0+codex.20260623113518 | Initial release | Full AIDP data-engineering surface in natural language: catalog discovery, SQL analysis, AI-in-SQL, Delta operations, pipelines, clusters, governance, agent flows, MLOps, migration, and workspace administration. |

## Install

Register the marketplace:

```bash
codex plugin marketplace add oracle-samples/oracle-aidp-samples \
    --ref main \
    --sparse ai/codex-plugins
```

Install a plugin:

```bash
codex plugin add oracle-ai-data-platform-workbench-databricks-migrator@oracle-aidp-codex
codex plugin add oracle-ai-data-platform-workbench-engineer-agent@oracle-aidp-codex
```

Verify:

```bash
codex plugin list
```

Start a new Codex thread after installing or upgrading plugins.

## Update

```bash
codex plugin marketplace upgrade oracle-aidp-codex
```

Then reinstall or refresh the plugin you want to test:

```bash
codex plugin add oracle-ai-data-platform-workbench-engineer-agent@oracle-aidp-codex
```

## Layout

```text
ai/codex-plugins/
|-- .agents/plugins/marketplace.json
|-- README.md
|-- TESTING.md
`-- plugins/
    |-- oracle-ai-data-platform-workbench-databricks-migrator/
    `-- oracle-ai-data-platform-workbench-engineer-agent/
```

Each plugin has its own `.codex-plugin/plugin.json`, README, license/privacy files, skills, and references or helper files.

## License

MIT - see each plugin's `LICENSE` file. Plugins are independent; each can be installed without the others.

## Contributing

These plugins live in the canonical Oracle Samples repo. Open an issue or PR at <https://github.com/oracle-samples/oracle-aidp-samples>.