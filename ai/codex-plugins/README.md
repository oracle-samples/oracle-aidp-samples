# Oracle AI Data Platform - Codex CLI Plugins

A marketplace of OpenAI Codex CLI plugins for Oracle AI Data Platform (AIDP).

## Marketplace

- **Name:** `oracle-aidp-codex`
- **Git repository manifest:** [`.agents/plugins/marketplace.json`](../../.agents/plugins/marketplace.json)
- **Local directory manifest:** [`.agents/plugins/marketplace.json`](./.agents/plugins/marketplace.json)

The repository-root manifest supports GitHub marketplace installation. The nested
manifest remains available when registering a local `ai/codex-plugins` checkout.

## Plugins

| Plugin | Version | Status | Purpose |
|---|---|---|---|
| [`oracle-ai-data-platform-workbench-databricks-migrator`](./plugins/oracle-ai-data-platform-workbench-databricks-migrator/) | 0.1.0 | Initial release | Plan and execute automated Databricks to AIDP migrations from Codex. |
| [`oracle-ai-data-platform-workbench-engineer-agent`](./plugins/oracle-ai-data-platform-workbench-engineer-agent/) | 0.1.0+codex.20260623113518 | Initial release | Full AIDP data-engineering surface in natural language: catalog discovery, SQL analysis, AI-in-SQL, Delta operations, pipelines, clusters, governance, agent flows, MLOps, migration, and workspace administration. |
| [`oracle-ai-data-platform-workbench-spark-connectors`](./plugins/oracle-ai-data-platform-workbench-spark-connectors/) | 0.7.0 | Db2 + Azure SQL connector guidance | Generate AIDP Apache Spark notebook connector code for Oracle, OCI, SaaS, JDBC, object storage, streaming, REST, Excel, and multi-cloud data sources. |
| [`ask-aidp`](./plugins/ask-aidp/) | 0.9.1 | Current release | Ask and operate Oracle AI Data Platform resources from Codex through aidp-cli, OCI-signed REST endpoints, and native SDK tools. |
| [`oracle-ai-data-platform-fusion-autopilot`](./plugins/oracle-ai-data-platform-fusion-autopilot/) | 0.1.0-alpha | Initial release | Build and operate curated Oracle Fusion ERP/HCM/SCM-to-AIDP medallion pipelines (BICC extracts, bronze/silver/gold content packs, guarded bootstrap/seed/incremental runs), gold marts, OAC datasets, and MCP-authored workbooks. |
| [`oracle-ai-data-platform-workbench-aws-migrator`](./plugins/oracle-ai-data-platform-workbench-aws-migrator/) | 0.2.0 | Initial release | Migrate an AWS data stack (Amazon S3, AWS Glue, Amazon Athena) to Oracle AIDP from Codex via an MCP server — inventory, plan, migrate, verify; deterministic translation with explicit review gates. |
| [`oracle-ai-data-platform-workbench-snowflake-migrator`](./plugins/oracle-ai-data-platform-workbench-snowflake-migrator/) | 0.28.0 | Initial release | Migrate a Snowflake database to Oracle AIDP through a fixed twelve-step runbook: in-AIDP discovery, a reviewed translation plan, target schemas and Delta tables, and one verified copy job per schema. |
| [`oracle-ai-data-platform-workbench-fabric-migrator`](./plugins/oracle-ai-data-platform-workbench-fabric-migrator/) | 0.1.0 | Initial release | Migrate a Microsoft Fabric workspace (notebooks, Warehouse T-SQL, Power Query M, Data Pipelines, shortcuts) to Oracle AIDP from Codex via an MCP server — inventory, plan, migrate, verify, publish (dry run); deterministic translation with explicit review gates. |
| [`oracle-ai-data-platform-workbench-informatica-migrator`](./plugins/oracle-ai-data-platform-workbench-informatica-migrator/) | 0.1.0 | Initial release | Migrate Informatica ETL (IDMC/IICS cloud mapping JSON, PowerCenter 10.x repository XML) to Oracle AIDP from Codex — mappings to PySpark notebooks, workflows to AIDP jobs with the session dependency graph, Quartz schedules and a declarable timezone; worklets expanded. Deterministic compiler, no model required; anything unsafe is reported with a named reason and a rebuild path, never guessed. |

## Install

Register the marketplace:

```bash
codex plugin marketplace add oracle-samples/oracle-aidp-samples \
    --ref main \
    --sparse .agents \
    --sparse ai/codex-plugins
```

Install a plugin:

```bash
codex plugin add oracle-ai-data-platform-workbench-databricks-migrator@oracle-aidp-codex
codex plugin add oracle-ai-data-platform-workbench-engineer-agent@oracle-aidp-codex
codex plugin add oracle-ai-data-platform-workbench-spark-connectors@oracle-aidp-codex
codex plugin add ask-aidp@oracle-aidp-codex
codex plugin add oracle-ai-data-platform-fusion-autopilot@oracle-aidp-codex
codex plugin add oracle-ai-data-platform-workbench-snowflake-migrator@oracle-aidp-codex
```

Verify:

```bash
codex plugin list
```

Restart the Codex app after installing or upgrading plugins, then start a new
thread so the updated skills and MCP servers are loaded.

## Update

```bash
codex plugin marketplace upgrade oracle-aidp-codex
```

Then reinstall or refresh the plugin you want to test:

```bash
codex plugin add oracle-ai-data-platform-workbench-engineer-agent@oracle-aidp-codex
codex plugin add oracle-ai-data-platform-workbench-spark-connectors@oracle-aidp-codex
codex plugin add ask-aidp@oracle-aidp-codex
```

Restart the Codex app after refreshing a plugin.

## Layout

```text
oracle-aidp-samples/
|-- .agents/plugins/marketplace.json
`-- ai/codex-plugins/
    |-- .agents/plugins/marketplace.json
    |-- README.md
    |-- TESTING.md
    `-- plugins/
        |-- oracle-ai-data-platform-workbench-databricks-migrator/
        |-- oracle-ai-data-platform-workbench-engineer-agent/
        |-- oracle-ai-data-platform-workbench-spark-connectors/
        |-- ask-aidp/
        `-- oracle-ai-data-platform-fusion-autopilot/
```

Each plugin has its own `.codex-plugin/plugin.json`, README, license/privacy files, skills, and references or helper files.

## License

MIT - see each plugin's `LICENSE` file. Plugins are independent; each can be installed without the others.

## Contributing

These plugins live in the canonical Oracle Samples repo. Open an issue or PR at <https://github.com/oracle-samples/oracle-aidp-samples>.
